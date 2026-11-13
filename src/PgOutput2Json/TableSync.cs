using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;
using NpgsqlTypes;

namespace PgOutput2Json
{
    /// <summary>
    /// The initial sync of the published tables, modelled on the table synchronization of PostgreSQL's own
    /// logical replication. It lives as long as one connection of the main stream.
    ///
    /// A table that is not "ready" is skipped by the main stream. A sync worker creates a replication slot
    /// for the table, copies the rows from the slot's snapshot, then streams the table's changes from the slot
    /// until it has caught up with the main stream. At a transaction boundary the main stream then stops, tells
    /// the worker its position, and waits until the worker has published the table's changes up to there.
    /// From that position on the table is "ready" and belongs to the main stream.
    ///
    /// The main stream only reads memory here. The workers do the database work on their own connections.
    /// </summary>
    internal sealed class TableSync : IAsyncDisposable
    {
        private const string KeepaliveSql = "SELECT pg_logical_emit_message(false, 'pg2j_keepalive', '')";

        private static readonly TimeSpan _retryDelay = TimeSpan.FromSeconds(10);

        // the main stream waits for about as long as the worker's last catch-up round took,
        // so the handover is requested only after a short round
        private static readonly TimeSpan _shortCatchUpRound = TimeSpan.FromSeconds(1);
        private const int MaxCatchUpRounds = 10;

        private sealed class Entry
        {
            private volatile string _state = TableSyncState.StateInit;

            /// <summary>Null for a table first met in the stream, until a worker has looked it up.</summary>
            public PublishedTable? Table { get; set; }
            public string State => _state;
            public ulong Lsn { get; private set; }

            public void Set(string state, ulong lsn)
            {
                Lsn = lsn; // the volatile write of the state publishes it
                _state = state;
            }
        }

        private sealed class Handover
        {
            public TaskCompletionSource<ulong> Position { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
            public TaskCompletionSource Done { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        private readonly IMessagePublisherFactory _publisherFactory;
        private readonly ReplicationListenerOptions _options;
        private readonly JsonOptions _jsonOptions;
        private readonly string _mainSlotName;
        private readonly ILoggerFactory? _loggerFactory;
        private readonly ILogger? _logger;

        private readonly ConcurrentDictionary<uint, Entry> _entries = new();
        private readonly Channel<uint> _queue = Channel.CreateUnbounded<uint>();
        private readonly ConcurrentQueue<Handover> _handovers = new();
        private readonly CancellationTokenSource _cts;

        private Task[] _workers = [];
        private long _mainPosition;

        private TableSync(IMessagePublisherFactory publisherFactory,
                          ReplicationListenerOptions options,
                          JsonOptions jsonOptions,
                          string mainSlotName,
                          ILoggerFactory? loggerFactory,
                          CancellationToken token)
        {
            _publisherFactory = publisherFactory;
            _options = options;
            _jsonOptions = jsonOptions;
            _mainSlotName = mainSlotName;
            _loggerFactory = loggerFactory;
            _logger = loggerFactory?.CreateLogger<TableSync>();
            _cts = CancellationTokenSource.CreateLinkedTokenSource(token);
        }

        /// <summary>
        /// Registers the published tables and starts the sync workers for the tables that need a copy.
        /// </summary>
        public static async Task<TableSync> StartAsync(IMessagePublisherFactory publisherFactory,
                                                       ReplicationListenerOptions options,
                                                       JsonOptions jsonOptions,
                                                       string mainSlotName,
                                                       ILoggerFactory? loggerFactory,
                                                       CancellationToken token)
        {
            var sync = new TableSync(publisherFactory, options, jsonOptions, mainSlotName, loggerFactory, token);

            try
            {
                var states = await TableSyncState.RegisterTablesAsync(options, sync._logger, token).ConfigureAwait(false);

                foreach (var state in states)
                {
                    var entry = new Entry { Table = state.Table };

                    entry.Set(state.State, state.Lsn);

                    sync._entries[state.Table.Oid] = entry;
                }

                if (!options.UseTemporarySlot)
                {
                    await sync.DropOrphanedSlotsAsync(token).ConfigureAwait(false);
                }

                foreach (var state in states.Where(s => s.State != TableSyncState.StateReady))
                {
                    sync._queue.Writer.TryWrite(state.Table.Oid);
                }

                if (options.CopyData)
                {
                    sync._workers = Enumerable.Range(0, options.MaxParallelCopyJobs)
                        .Select(_ => Task.Run(() => sync.RunWorkerAsync(sync._cts.Token), CancellationToken.None))
                        .ToArray();
                }
            }
            catch
            {
                await sync.DisposeAsync().ConfigureAwait(false);
                throw;
            }

            return sync;
        }

        /// <summary>
        /// Called by the main stream for every change of a table: false if the change belongs to a sync worker.
        /// </summary>
        public bool ShouldPublish(uint relationId, ulong transactionFinalLsn)
        {
            if (!_entries.TryGetValue(relationId, out var entry))
            {
                // a table published after the start
                entry = new Entry();

                if (!_options.CopyData)
                {
                    // streamed only - it is registered as ready on the next start
                    entry.Set(TableSyncState.StateReady, 0);
                }

                _entries[relationId] = entry;

                if (_options.CopyData)
                {
                    _queue.Writer.TryWrite(relationId);
                }
            }

            // the worker published the transactions that ended at or before the handover position;
            // the same comparison as should_apply_changes_for_rel in PostgreSQL
            return entry.State == TableSyncState.StateReady && transactionFinalLsn >= entry.Lsn;
        }

        /// <summary>
        /// Called by the main stream between transactions with the end position of everything it has processed.
        /// Returns true if a worker waits for a handover.
        /// </summary>
        public bool AtBoundary(ulong lsn)
        {
            Volatile.Write(ref _mainPosition, (long)lsn);

            return !_handovers.IsEmpty;
        }

        /// <summary>
        /// Tells the waiting workers the position of the main stream and waits until they have published
        /// their tables' changes up to it. The main stream must have confirmed its own batch before.
        /// </summary>
        public async Task HandOverAsync(ulong lsn, CancellationToken token)
        {
            while (_handovers.TryDequeue(out var handover))
            {
                handover.Position.TrySetResult(lsn);

                // set by the worker even if it fails; the table then stays with the worker for another try
                await handover.Done.Task.WaitAsync(token).ConfigureAwait(false);
            }
        }

        public async ValueTask DisposeAsync()
        {
            await _cts.TryCancelAsync(_logger).ConfigureAwait(false);

            try
            {
                await Task.WhenAll(_workers).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.SafeLogError(ex, "Error stopping the table sync workers");
            }

            _cts.TryDispose(_logger);
        }

        /// <summary>
        /// The names of the sync slots of a main slot start with this, followed by the oid of the table.
        /// A slot name is limited to 63 characters, so a long main slot name is shortened and made unique with a hash.
        /// </summary>
        internal static string GetSyncSlotPrefix(string mainSlotName)
        {
            const string suffix = "_sync_";
            const int maxLength = 63 - 6 - 10; // the suffix and the longest oid

            var name = mainSlotName.Length <= maxLength
                ? mainSlotName
                : $"{mainSlotName[..(maxLength - 9)]}_{unchecked((uint)MurmurHash2.Hash(mainSlotName)):x8}";

            return name + suffix;
        }

        /// <summary>
        /// The relation of a row change, null for any other message.
        /// </summary>
        internal static RelationMessage? GetRelation(PgOutputReplicationMessage message)
        {
            return message switch
            {
                InsertMessage insert => insert.Relation,
                UpdateMessage update => update.Relation,
                DeleteMessage delete => delete.Relation,
                _ => null,
            };
        }

        private async Task RunWorkerAsync(CancellationToken token)
        {
            try
            {
                await foreach (var oid in _queue.Reader.ReadAllAsync(token).ConfigureAwait(false))
                {
                    try
                    {
                        await SyncTableAsync(oid, token).ConfigureAwait(false);
                    }
                    catch (Exception ex) when (!token.IsCancellationRequested)
                    {
                        MetricsHelper.IncrementErrorCounter();

                        _logger.SafeLogError(ex, "Error in the initial sync of table {Table}. Trying again in 10 seconds...",
                            _entries[oid].Table?.TableName ?? oid.ToString());

                        await Task.Delay(_retryDelay, token).ConfigureAwait(false);

                        _queue.Writer.TryWrite(oid);
                    }
                }
            }
            catch (Exception) when (token.IsCancellationRequested)
            {
                // stopping
            }
        }

        private async Task SyncTableAsync(uint oid, CancellationToken token)
        {
            var entry = _entries[oid];

            var table = entry.Table ??= await TableSyncState.FindPublishedTableAsync(_options, oid, token).ConfigureAwait(false);

            if (table == null)
            {
                _logger?.LogWarning("Relation {Oid} was not found in the publications. Its changes are published without an initial copy.", oid);

                entry.Set(TableSyncState.StateReady, 0);
                return;
            }

            var slotName = GetSyncSlotPrefix(_mainSlotName) + oid;

            Handover? handover = null;

            var connection = new LogicalReplicationConnection(_options.ConnectionString);

            await using var connectionScope = connection.ConfigureAwait(false);

            var publisher = _publisherFactory.CreateMessagePublisher(_options, _mainSlotName, _loggerFactory);

            await using var publisherScope = publisher.ConfigureAwait(false);

            try
            {
                await connection.Open(token).ConfigureAwait(false);

                PgOutputReplicationSlot slot;
                ulong position; // the end of everything published for the table, by the copy or from the slot

                var resumeFrom = entry.State == TableSyncState.StateCopied
                    ? await GetSlotPositionAsync(slotName, token).ConfigureAwait(false)
                    : null;

                if (resumeFrom != null)
                {
                    slot = new PgOutputReplicationSlot(slotName);
                    position = resumeFrom.Value;

                    _logger?.LogInformation("Resuming the sync of table {Table} from slot {Slot}", table.TableName, slotName);
                }
                else
                {
                    // start from zero: what an unfinished copy left at the publisher is published again
                    await DropSlotAsync(slotName, token).ConfigureAwait(false);

                    await SetStateAsync(entry, table, TableSyncState.StateCopying, 0, token).ConfigureAwait(false);

                    // the exported snapshot lives until the next command on this connection
                    var created = await connection.CreatePgOutputReplicationSlot(slotName,
                                                                                 _options.UseTemporarySlot,
                                                                                 LogicalSlotSnapshotInitMode.Export,
                                                                                 cancellationToken: token)
                        .ConfigureAwait(false);

                    _logger?.LogInformation("Copying table {Table}, RowFilter: {RowFilter}", table.TableName, table.RowFilter);

                    var rows = await TableCopy.CopyAsync(publisher, _options, _jsonOptions, table, created.SnapshotName!, _logger, token)
                        .ConfigureAwait(false);

                    _logger?.LogInformation("Copied {Rows} rows of table {Table}", rows, table.TableName);

                    slot = created;
                    position = (ulong)created.ConsistentPoint;

                    await SetStateAsync(entry, table, TableSyncState.StateCopied, position, token).ConfigureAwait(false);
                }

                var writer = new JsonWriter(_jsonOptions, _options);
                var message = new ReplicationMessage { HasRelationChanged = true };

                var lastWalEnd = default(NpgsqlLogSequenceNumber);
                var unconfirmed = 0;

                // first catch up with the main stream, then ask it to stop and stream up to where it stopped
                var target = (ulong)Volatile.Read(ref _mainPosition);
                var round = Stopwatch.StartNew();
                var rounds = 1;

                async Task ConfirmAsync()
                {
                    if (unconfirmed > 0)
                    {
                        await publisher.ConfirmAsync(token).ConfigureAwait(false);
                        unconfirmed = 0;
                    }

                    if ((ulong)lastWalEnd > 0)
                    {
                        connection.SetReplicationStatus(lastWalEnd);
                        await connection.SendStatusUpdate(token).ConfigureAwait(false);
                    }
                }

                async Task<bool> IsDoneAsync()
                {
                    while (position >= target)
                    {
                        if (handover != null)
                        {
                            await ConfirmAsync().ConfigureAwait(false);
                            return true;
                        }

                        // the main stream has moved on while this round ran - go again if it was a long one
                        var mainPosition = (ulong)Volatile.Read(ref _mainPosition);

                        if (position < mainPosition && round.Elapsed > _shortCatchUpRound && rounds < MaxCatchUpRounds)
                        {
                            target = mainPosition;
                            rounds++;
                            round.Restart();
                            continue;
                        }

                        await ConfirmAsync().ConfigureAwait(false);

                        handover = new Handover();
                        _handovers.Enqueue(handover);

                        // the main stream answers between transactions, so make sure it gets there on a quiet database too
                        await ExecuteAsync(KeepaliveSql, null, token).ConfigureAwait(false);

                        target = await handover.Position.Task.WaitAsync(token).ConfigureAwait(false);
                    }

                    return false;
                }

                // everything up to the position is at the publisher and confirmed: the main stream takes over
                async Task FinishAsync()
                {
                    await SetStateAsync(entry, table, TableSyncState.StateReady, position, token).ConfigureAwait(false);

                    handover!.Done.TrySetResult();

                    _logger?.LogInformation("Table {Table} is synced, the main stream continues from {Lsn}", table.TableName, new NpgsqlLogSequenceNumber(position));
                }

                if (await IsDoneAsync().ConfigureAwait(false))
                {
                    await FinishAsync().ConfigureAwait(false);
                }
                else
                {
                    var replicationOptions = new PgOutputReplicationOptions(_options.PublicationNames, PgOutputProtocolVersion.V1, messages: true);

                    // The stream carries the same messages as the main stream, so the transaction or message
                    // that gave the main stream its position arrives here too, and the loop ends.
                    await foreach (var msg in connection.StartReplication(slot, replicationOptions, token).ConfigureAwait(false))
                    {
                        switch (msg)
                        {
                            case RelationMessage relation:
                                if (relation.RelationId == oid) message.HasRelationChanged = true;
                                continue;

                            case BeginMessage begin:
                                message.CommitTimeStamp = begin.TransactionCommitTimestamp;
                                message.TransactionFinalLsn = (ulong)begin.TransactionFinalLsn;
                                message.MessageNo = 0;
                                continue;

                            case CommitMessage commit:
                                lastWalEnd = msg.WalEnd;
                                position = (ulong)commit.TransactionEndLsn;
                                break;

                            case LogicalDecodingMessage nonTransactional when (nonTransactional.Flags & 1) == 0:
                                lastWalEnd = msg.WalEnd;
                                position = (ulong)nonTransactional.MessageLsn;
                                break;

                            case LogicalDecodingMessage:
                                message.MessageNo++; // counted, so "n" is what the main stream would have sent
                                continue;

                            case InsertMessage or UpdateMessage or DeleteMessage:
                                message.MessageNo++;

                                if (GetRelation(msg)!.RelationId == oid)
                                {
                                    message.Message = msg;

                                    var jsonMessage = await writer.WriteMessageAsync(message, token).ConfigureAwait(false);

                                    jsonMessage.IsTableSync = true;
                                    message.HasRelationChanged = false;

                                    await publisher.PublishAsync(jsonMessage, token).ConfigureAwait(false);

                                    MetricsHelper.IncrementPublishCounter();

                                    lastWalEnd = msg.WalEnd;

                                    if (++unconfirmed >= _options.BatchSize)
                                    {
                                        await ConfirmAsync().ConfigureAwait(false);
                                    }
                                }
                                continue;

                            default:
                                continue;
                        }

                        if (await IsDoneAsync().ConfigureAwait(false))
                        {
                            // the main stream is waiting, so it is released before the stream is closed
                            await FinishAsync().ConfigureAwait(false);
                            break;
                        }
                    }
                }

            }
            finally
            {
                handover?.Done.TrySetResult();
            }

            // closing the connection is the quick way out of a replication stream, and it frees the slot
            await connection.DisposeAsync().ConfigureAwait(false);

            if (!_options.UseTemporarySlot)
            {
                await DropSyncedSlotAsync(slotName, token).ConfigureAwait(false);
            }
        }

        private async Task DropSyncedSlotAsync(string slotName, CancellationToken token)
        {
            // the slot stays active until the server notices that the connection is gone
            for (var attempt = 1; ; attempt++)
            {
                try
                {
                    await DropSlotAsync(slotName, token).ConfigureAwait(false);
                    return;
                }
                catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.ObjectInUse && attempt < 50)
                {
                    await Task.Delay(100, token).ConfigureAwait(false);
                }
                catch (Exception ex) when (!token.IsCancellationRequested)
                {
                    _logger?.LogWarning(ex, "Could not drop the sync slot {Slot}. It is dropped on the next start.", slotName);
                    return;
                }
            }
        }

        private async Task SetStateAsync(Entry entry, PublishedTable table, string state, ulong lsn, CancellationToken token)
        {
            await TableSyncState.SetStateAsync(_options, table, state, lsn, token).ConfigureAwait(false);

            entry.Set(state, lsn);
        }

        /// <summary>
        /// Drops the sync slots left behind by earlier runs, except those of the tables whose copy is done.
        /// </summary>
        private async Task DropOrphanedSlotsAsync(CancellationToken token)
        {
            var prefix = GetSyncSlotPrefix(_mainSlotName);

            try
            {
                var slotNames = new List<string>();

                using (var cn = new NpgsqlConnection(_options.ConnectionString))
                {
                    await cn.OpenAsync(token).ConfigureAwait(false);

                    using var cmd = new NpgsqlCommand("SELECT slot_name::text FROM pg_replication_slots WHERE NOT active AND left(slot_name::text, @length) = @prefix", cn);

                    cmd.Parameters.AddWithValue("length", prefix.Length);
                    cmd.Parameters.AddWithValue("prefix", prefix);

                    using var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);

                    while (await reader.ReadAsync(token).ConfigureAwait(false))
                    {
                        slotNames.Add(reader.GetString(0));
                    }
                }

                foreach (var slotName in slotNames)
                {
                    if (!uint.TryParse(slotName.AsSpan(prefix.Length), out var oid)) continue;

                    if (_entries.TryGetValue(oid, out var entry) && entry.State == TableSyncState.StateCopied) continue;

                    await DropSlotAsync(slotName, token).ConfigureAwait(false);

                    _logger?.LogInformation("Dropped the orphaned sync slot {Slot}", slotName);
                }
            }
            catch (PostgresException ex)
            {
                _logger?.LogWarning(ex, "Could not drop the orphaned sync slots of slot {Slot}", _mainSlotName);
            }
        }

        private async Task<ulong?> GetSlotPositionAsync(string slotName, CancellationToken token)
        {
            using var cn = new NpgsqlConnection(_options.ConnectionString);

            await cn.OpenAsync(token).ConfigureAwait(false);

            using var cmd = new NpgsqlCommand("SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = @slot_name", cn);

            cmd.Parameters.AddWithValue("slot_name", slotName);

            var result = await cmd.ExecuteScalarAsync(token).ConfigureAwait(false);

            return result is NpgsqlLogSequenceNumber lsn ? (ulong)lsn : null;
        }

        private Task DropSlotAsync(string slotName, CancellationToken token)
        {
            return ExecuteAsync("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = @slot_name", slotName, token);
        }

        private async Task ExecuteAsync(string sql, string? slotName, CancellationToken token)
        {
            using var cn = new NpgsqlConnection(_options.ConnectionString);

            await cn.OpenAsync(token).ConfigureAwait(false);

            using var cmd = new NpgsqlCommand(sql, cn);

            if (slotName != null) cmd.Parameters.AddWithValue("slot_name", slotName);

            await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }
    }
}
