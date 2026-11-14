using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

using Npgsql;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;
using NpgsqlTypes;

namespace PgOutput2Json
{
    internal sealed class ReplicationListener
    {
        private class ConnectionState(CancellationToken cancellationToken, ILogger? logger) : IAsyncDisposable
        {
            private readonly ILogger? _logger = logger;

            public LogicalReplicationConnection? Connection { get; set; }
            public IMessagePublisher? MessagePublisher { get; set; }
            public TableSync? TableSync { get; set; }
            public CancellationTokenSource LinkedCts { get; } = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            public CancellationToken CancellationToken { get; } = cancellationToken;
            public int UnconfirmedCount { get; set; }
            public NpgsqlLogSequenceNumber LastWalEnd { get; set; }

            public Timer? IdleConfirmTimer { get; set; }
            public Timer? IdleWalMessageTimer { get; set; }

            public bool IsDisposed { get; private set; }

            public async ValueTask DisposeAsync()
            {
                IsDisposed = true;

                await LinkedCts.TryCancelAsync(_logger).ConfigureAwait(false);
                LinkedCts.TryDispose(_logger);

                await IdleConfirmTimer.TryDisposeAsync(_logger).ConfigureAwait(false);
                IdleConfirmTimer = null;

                await IdleWalMessageTimer.TryDisposeAsync(_logger).ConfigureAwait(false);
                IdleWalMessageTimer = null;

                await TableSync.TryDisposeAsync(_logger).ConfigureAwait(false);
                TableSync = null;

                await MessagePublisher.TryDisposeAsync(_logger).ConfigureAwait(false);
                MessagePublisher = null;

                await Connection.TryDisposeAsync(_logger).ConfigureAwait(false);
                Connection = null;
            }
        }

        private readonly ILoggerFactory? _loggerFactory;
        private readonly ILogger<ReplicationListener>? _logger;

        private readonly ReplicationListenerOptions _options;
        private readonly JsonOptions _jsonOptions;

        private readonly JsonWriter _writer;

        private readonly IMessagePublisherFactory _messagePublisherFactory;

        private readonly AsyncLock _lock = new();

        public ReplicationListener(IMessagePublisherFactory messagePublisherFactory,
                                   ReplicationListenerOptions options,
                                   JsonOptions jsonOptions,
                                   ILoggerFactory? loggerFactory)
        {
            _messagePublisherFactory = messagePublisherFactory;
            _options = options;
            _jsonOptions = jsonOptions;
            _writer = new JsonWriter(jsonOptions, options);

            _loggerFactory = loggerFactory;
            _logger = loggerFactory?.CreateLogger<ReplicationListener>();

            // TODO: see if DataSourceBuilder can be used
            if (_loggerFactory != null)
            {
                NpgsqlLoggingConfiguration.InitializeLogging(_loggerFactory);
            }
        }

        public async Task ListenForChangesAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var state = new ConnectionState(cancellationToken, _logger);

                using (ExecutionContext.SuppressFlow())
                {
                    state.IdleConfirmTimer = new Timer(IdleTimerCallback, state, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);

                    if (_options.IdleWalMessageInterval > TimeSpan.Zero
                        && _options.IdleWalMessageInterval != Timeout.InfiniteTimeSpan)
                    {
                        state.IdleWalMessageTimer = new Timer(IdleWalMessageTimerCallback, state, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
                    }
                }

                try
                {
                    state.Connection = new LogicalReplicationConnection(_options.ConnectionString)
                    {
                        WalReceiverStatusInterval = Timeout.InfiniteTimeSpan // we are sending status manually
                    };

                    await state.Connection.Open(cancellationToken)
                        .ConfigureAwait(false);

                    _logger.SafeLogInfo("Connected to PostgreSQL");

                    var slotName = string.IsNullOrWhiteSpace(_options.ReplicationSlotName)
                            ? $"pg2j_{Guid.NewGuid().ToString().Replace("-", "")}"
                            : _options.ReplicationSlotName;

                    PgOutputReplicationSlot slot;

                    if (!_options.UseTemporarySlot)
                    {
                        // the table sync must not start if this listener cannot get the slot
                        await EnsureSlotIsFreeAsync(slotName, cancellationToken).ConfigureAwait(false);

                        slot = new PgOutputReplicationSlot(_options.ReplicationSlotName);
                    }
                    else
                    {
                        slot = await state.Connection.CreatePgOutputReplicationSlot(slotName, true, cancellationToken: cancellationToken)
                            .ConfigureAwait(false);
                    }

                    // registers the published tables and, with the initial copy on, syncs the new ones in the background
                    var tableSync = state.TableSync = await TableSync.StartAsync(_messagePublisherFactory, _options, _jsonOptions, slotName, _loggerFactory, cancellationToken)
                        .ConfigureAwait(false);

                    state.MessagePublisher = _messagePublisherFactory.CreateMessagePublisher(_options, slotName, _loggerFactory);

                    // (0, 0) unless the publisher saves its position with the data
                    var lastPublished = await state.MessagePublisher.GetLastPublishedWalSeqAsync(cancellationToken).ConfigureAwait(false);

                    var replicationMessage = new ReplicationMessage();

                    // the relations whose schema goes out with their next published row
                    var changedRelations = new HashSet<uint>();

                    // Between transactions: a sync worker that has caught up gets its table handed over here.
                    // This listener's own batch is confirmed first, so that the worker's messages follow it at
                    // the output, and a store's open transaction does not block the worker.
                    async Task AtBoundaryAsync(ulong lsn)
                    {
                        if (!tableSync.AtBoundary(lsn)) return;

                        state.IdleConfirmTimer?.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);

                        await ConfirmAsync(state).ConfigureAwait(false);

                        await tableSync.HandOverAsync(lsn, state.LinkedCts.Token).ConfigureAwait(false);
                    }

                    var replicationOptions = new PgOutputReplicationOptions(_options.PublicationNames, PgOutputProtocolVersion.V1, messages: true);

                    // start the keeplive timer just before starting the replication
                    state.IdleWalMessageTimer?.Change(_options.IdleWalMessageInterval, Timeout.InfiniteTimeSpan);

                    long skippedCount = 0;
                    bool startingUp = true; // skip messages only during startup

                    // linkedCts.Token is used only in this foreach loop,
                    // since lock ensures idle confirm cannot happen at the same time
                    await foreach (var message in state.Connection.StartReplication(slot, replicationOptions, state.LinkedCts.Token)
                        .ConfigureAwait(false))
                    {
                        using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false))
                        {
                            //_logger?.LogWarning("{Type} {WalStart}/{MesssageNo}", message.GetType().Name, message.WalStart, messageNo);

                            state.IdleConfirmTimer?.Change(_options.BatchWaitTime, Timeout.InfiniteTimeSpan);
                            state.IdleWalMessageTimer?.Change(_options.IdleWalMessageInterval, Timeout.InfiniteTimeSpan);

                            if (message is RelationMessage rel)
                            {
                                changedRelations.Add(rel.RelationId);

                                ValidatePartitionKeyColumns(rel);

                                // Relation Message has WalEnd=0/0
                                continue;
                            }

                            if (message is BeginMessage beginMsg)
                            {
                                replicationMessage.CommitTimeStamp = beginMsg.TransactionCommitTimestamp;
                                replicationMessage.TransactionFinalLsn = (ulong)beginMsg.TransactionFinalLsn;
                                replicationMessage.MessageNo = 0UL;
                                continue;
                            }

                            if (message is CommitMessage commitMsg)
                            {
                                // DO NOT REMOVE THIS
                                // This is checked multiple times, we must confirm this WalEnd too,
                                // since the whole transaction will repeat otherwise.
                                state.LastWalEnd = message.WalEnd;

                                await AtBoundaryAsync((ulong)commitMsg.TransactionEndLsn).ConfigureAwait(false);
                                continue;
                            }

                            if (message is LogicalDecodingMessage logicalMsg && logicalMsg.Prefix == "pg2j_keepalive")
                            {
                                state.LastWalEnd = message.WalEnd;

                                await AtBoundaryAsync((ulong)logicalMsg.MessageLsn).ConfigureAwait(false);
                                continue;
                            }

                            if (message is LogicalDecodingMessage nonTxMsg && (nonTxMsg.Flags & 1) == 0)
                            {
                                // A non-transactional message is sent outside of Begin/Commit, as soon as it is decoded,
                                // so it has no transaction of its own - without this it would inherit the deduplication
                                // key of the previous transaction (or 0/1 after a reconnect, and be skipped as a duplicate).
                                // Its own LSN is a valid key: it is decoded in WAL order, and every transaction sent
                                // after it commits at a higher LSN. The counter is incremented to 1 below.
                                replicationMessage.TransactionFinalLsn = (ulong)nonTxMsg.MessageLsn;
                                replicationMessage.CommitTimeStamp = nonTxMsg.ServerClock;
                                replicationMessage.MessageNo = 0UL;
                            }

                            if (message is InsertMessage ||
                                message is DefaultUpdateMessage ||
                                message is FullUpdateMessage ||
                                message is IndexUpdateMessage ||
                                message is KeyDeleteMessage ||
                                message is FullDeleteMessage ||
                                message is LogicalDecodingMessage)
                            {
                                replicationMessage.Message = message;
                                replicationMessage.MessageNo++;

                                if (startingUp
                                    && _options.UseDeduplication
                                    && (replicationMessage.TransactionFinalLsn, replicationMessage.MessageNo).CompareTo(lastPublished) <= 0)
                                {
                                    // already processed
                                    skippedCount++;
                                    continue;
                                }

                                if (startingUp && skippedCount > 0)
                                {
                                    _logger?.LogWarning("Deduplication enabled, skipped {SkippedCount} already published messages.", skippedCount);
                                }

                                startingUp = false;

                                var relation = TableSync.GetRelation(message);

                                // Our own bookkeeping tables are part of a FOR ALL TABLES publication.
                                // A table that is being synced is published by its sync worker.
                                if (relation == null
                                    || (relation.Namespace != TableSyncState.Schema
                                        && tableSync.ShouldPublish(relation.RelationId, replicationMessage.TransactionFinalLsn)))
                                {
                                    replicationMessage.HasRelationChanged = relation != null && changedRelations.Remove(relation.RelationId);

                                    var jsonMessage = await _writer.WriteMessageAsync(replicationMessage, cancellationToken)
                                        .ConfigureAwait(false);

                                    await state.MessagePublisher.PublishAsync(jsonMessage, cancellationToken)
                                        .ConfigureAwait(false);

                                    MetricsHelper.IncrementPublishCounter();
                                }
                            }

                            // set the lastWalEnd to be sent in status update only after the message was published
                            state.LastWalEnd = message.WalEnd;

                            if (message is LogicalDecodingMessage outsideTransaction && (outsideTransaction.Flags & 1) == 0)
                            {
                                await AtBoundaryAsync((ulong)outsideTransaction.MessageLsn).ConfigureAwait(false);
                            }

                            if (++state.UnconfirmedCount < _options.BatchSize)
                            {
                                continue;
                            }

                            state.IdleConfirmTimer?.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);

                            await ConfirmAsync(state).ConfigureAwait(false);

                            _logger.SafeLogDebug("Confirmed PostgreSQL");
                        }
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    _logger.SafeLogWarn("Stopping ReplicationListener - cancellation requested");
                    break;
                }
                catch (OperationCanceledException) when (state.LinkedCts.IsCancellationRequested)
                {
                    // cancelled because idle confirm failed, nothing to do - error has been logged already
                }
                catch (Exception ex)
                {
                    if (ex is SlotTakenException || (ex is PostgresException pgEx && pgEx.SqlState == "55006"))
                    {
                        _logger.SafeLogWarn("Slot taken - waiting for {Seconds} seconds...", _options.ReconnectDelay.TotalSeconds);
                    }
                    else
                    {
                        MetricsHelper.IncrementErrorCounter();
                        _logger.SafeLogError(ex, "Error in replication listener. Waiting for {Seconds} seconds...", _options.ReconnectDelay.TotalSeconds);
                    }
                }
                finally
                {
                    // we don't use cancellation token here, as we want to dispose always
                    using (await _lock.LockAsync(CancellationToken.None).ConfigureAwait(false))
                    {
                        await state.TryDisposeAsync(_logger).ConfigureAwait(false);
                    }
                }

                await DelayAsync(_options.ReconnectDelay, cancellationToken)
                    .ConfigureAwait(false);
            }

            _logger.SafeLogInfo("Disconnected from PostgreSQL");
        }

        private sealed class SlotTakenException(string message) : Exception(message);

        private async Task EnsureSlotIsFreeAsync(string slotName, CancellationToken token)
        {
            using var cn = new NpgsqlConnection(_options.ConnectionString);

            await cn.OpenAsync(token).ConfigureAwait(false);

            using var cmd = new NpgsqlCommand("SELECT active FROM pg_replication_slots WHERE slot_name = @slot_name", cn);

            cmd.Parameters.AddWithValue("slot_name", slotName);

            var active = await cmd.ExecuteScalarAsync(token).ConfigureAwait(false);

            if (active == null) throw new InvalidOperationException($"Replication slot {slotName} does not exist");

            if ((bool)active) throw new SlotTakenException($"Replication slot {slotName} is in use");
        }

        /// <summary>
        /// Partition key values are read from the key row for deletes, which contains only the replica
        /// identity columns. A partition key column outside the replica identity therefore yields an empty
        /// partition key for deletes, routing them to a different partition than the row's inserts and updates.
        /// The relation message is the only place where the actual replica identity is known, so it is checked here.
        /// pgoutput sends the relation once per connection (and again on schema change), so this is logged rarely.
        /// </summary>
        private void ValidatePartitionKeyColumns(RelationMessage rel)
        {
            if (_logger == null || _options.PartitionKeyColumns.Count == 0) return;

            var tableName = $"{rel.Namespace}.{rel.RelationName}";

            if (!_options.PartitionKeyColumns.TryGetValue(tableName, out var partitionCols)) return;

            _options.IncludedColumns.TryGetValue(tableName, out var includedCols);

            foreach (var partitionCol in partitionCols)
            {
                RelationMessage.Column? column = null;

                foreach (var col in rel.Columns)
                {
                    if (col.ColumnName == partitionCol)
                    {
                        column = col;
                        break;
                    }
                }

                if (column == null)
                {
                    _logger.LogError("Partition key column {Column} does not exist in table {Table} - partition key will be incomplete for all messages",
                        partitionCol, tableName);
                }
                else if (includedCols != null && !includedCols.Contains(partitionCol))
                {
                    _logger.LogError("Partition key column {Column} of table {Table} is not in the included columns - partition key will be incomplete for all messages",
                        partitionCol, tableName);
                }
                else if ((column.Value.Flags & RelationMessage.Column.ColumnFlags.PartOfKey) == 0)
                {
                    _logger.LogError("Partition key column {Column} of table {Table} is not part of the replica identity - deletes will not carry the partition key " +
                        "and may be routed to a different partition. Use primary key columns for partitioning, or set REPLICA IDENTITY FULL on the table.",
                        partitionCol, tableName);
                }
            }
        }

        private static async Task ConfirmAsync(ConnectionState cs)
        {
            if (cs.UnconfirmedCount > 0 && cs.MessagePublisher != null)
            {
                await cs.MessagePublisher.ConfirmAsync(cs.CancellationToken).ConfigureAwait(false);
            }

            cs.UnconfirmedCount = 0;

            if ((ulong)cs.LastWalEnd > 0 && cs.Connection != null)
            {
                cs.Connection.SetReplicationStatus(cs.LastWalEnd);

                await cs.Connection.SendStatusUpdate(cs.CancellationToken)
                    .ConfigureAwait(false);
            }
        }

#pragma warning disable VSTHRD100 // Avoid async void methods
        private async void IdleTimerCallback(object? state)
        {
            var cs = (ConnectionState)state!;

            try
            {
                if (cs.CancellationToken.IsCancellationRequested)
                {
                    return;
                }

                using (await _lock.LockAsync(cs.CancellationToken).ConfigureAwait(false))
                {
                    // check for disposal inside the lock
                    if (!cs.IsDisposed && cs.Connection != null && cs.MessagePublisher != null)
                    {
                        await ConfirmAsync(cs).ConfigureAwait(false);
                        _logger.SafeLogDebug("Idle Confirmed PostgreSQL");
                    }
                }
            }
            catch (OperationCanceledException) when (cs.CancellationToken.IsCancellationRequested)
            {
                // stopping - nothing to do
            }
            catch (Exception ex)
            {
                try
                {
                    MetricsHelper.IncrementErrorCounter();
                    _logger.SafeLogError(ex, "Error confirming published messages. Waiting for {Seconds} seconds...", _options.ReconnectDelay.TotalSeconds);

                    using (await _lock.LockAsync(cs.CancellationToken).ConfigureAwait(false))
                    {
                        if (!cs.IsDisposed)
                        {
                            // if force confirm fails, stop the replication loop, and dispose the publisher
                            await cs.LinkedCts.TryCancelAsync(_logger).ConfigureAwait(false);
                        }
                    }
                }
                catch (Exception exx)
                {
                    _logger.SafeLogError(exx, "Error cancelling link token source");
                }
            }
        }
#pragma warning restore VSTHRD100 // Avoid async void methods

#pragma warning disable VSTHRD100 // Avoid async void methods
        private async void IdleWalMessageTimerCallback(object? state)
        {
            var cs = (ConnectionState)state!;

            if (cs.CancellationToken.IsCancellationRequested)
            {
                return;
            }

            try
            {
                await using var conn = new NpgsqlConnection(_options.ConnectionString);
                await conn.OpenAsync(cs.CancellationToken).ConfigureAwait(false);

                await using var cmd = new NpgsqlCommand(
                    "SELECT pg_logical_emit_message(false, 'pg2j_keepalive', '')", conn);

                await cmd.ExecuteNonQueryAsync(cs.CancellationToken).ConfigureAwait(false);

                _logger.SafeLogDebug("Emitted idle WAL pg2j_keepalive message");
            }
            catch (OperationCanceledException) when (cs.CancellationToken.IsCancellationRequested)
            {
                // stopping - nothing to do
                return;
            }
            catch (Exception ex)
            {
                _logger.SafeLogError(ex, "Error emitting idle WAL message");
            }

            // Reschedule after a failure too: the main loop reschedules only when a message arrives, and
            // on an idle database none would - the keepalives would stop until the next real change.
            try
            {
                using (await _lock.LockAsync(cs.CancellationToken).ConfigureAwait(false))
                {
                    // checking for disposal inside the lock
                    if (!cs.IsDisposed && cs.IdleWalMessageTimer != null)
                    {
                        cs.IdleWalMessageTimer.Change(_options.IdleWalMessageInterval, Timeout.InfiniteTimeSpan);
                    }
                }
            }
            catch (OperationCanceledException) when (cs.CancellationToken.IsCancellationRequested)
            {
                // stopping - nothing to do
            }
            catch (Exception ex)
            {
                _logger.SafeLogError(ex, "Error rescheduling idle WAL message");
            }
        }
#pragma warning restore VSTHRD100 // Avoid async void methods

        private async Task DelayAsync(TimeSpan time, CancellationToken cancellationToken)
        {
            try
            {
                await Task.Delay(time, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // ignore if task was cancelled 
            }
            catch (Exception ex)
            {
                _logger.SafeLogError(ex, "Error while waiting to reconnect");
            }
        }
    }
}
