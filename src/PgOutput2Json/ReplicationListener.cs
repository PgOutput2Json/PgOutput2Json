using System;
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
                        slot = new PgOutputReplicationSlot(_options.ReplicationSlotName);
                    }
                    else
                    {
                        slot = await state.Connection.CreatePgOutputReplicationSlot(slotName, true, cancellationToken: cancellationToken)
                            .ConfigureAwait(false);
                    }

                    // start data export after creating the temporary replication slot
                    await DataExporter.MaybeExportDataAsync(_messagePublisherFactory, _options, _jsonOptions, slotName, _loggerFactory, cancellationToken).ConfigureAwait(false);

                    state.MessagePublisher = _messagePublisherFactory.CreateMessagePublisher(_options, slotName, _loggerFactory);

                    // virtual lsn is start lsn + msg number
                    var lastVirtualLsn = new NpgsqlLogSequenceNumber(await state.MessagePublisher.GetLastPublishedWalSeqAsync(cancellationToken).ConfigureAwait(false));

                    var lastWalStart = new NpgsqlLogSequenceNumber(0);

                    // this counts messages with the same WalStart
                    var messageNo = 0UL;

                    var replicationOptions = new PgOutputReplicationOptions(_options.PublicationNames, PgOutputProtocolVersion.V1, messages: true);

                    var replicationMessage = new ReplicationMessage { HasRelationChanged = true, CommitTimeStamp = DateTime.UtcNow, };

                    // start the keeplive timer just before starting the replication
                    state.IdleWalMessageTimer?.Change(_options.IdleWalMessageInterval, Timeout.InfiniteTimeSpan);

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
                                replicationMessage.HasRelationChanged = true;

                                // Relation Message has WalEnd=0/0
                                continue;
                            }

                            if (message is BeginMessage beginMsg)
                            {
                                replicationMessage.CommitTimeStamp = beginMsg.TransactionCommitTimestamp;
                                continue;
                            }

                            if (message is CommitMessage commitMsg)
                            {
                                // DO NOT REMOVE THIS
                                // This is checked multiple times, we must confirm this WalEnd too,
                                // since the whole transaction will repeat otherwise.
                                state.LastWalEnd = message.WalEnd;
                                continue;
                            }

                            if (lastWalStart != message.WalStart)
                            {
                                lastWalStart = message.WalStart;
                                messageNo = 0UL;
                            }
                            else
                            {
                                messageNo++;
                            }

                            var virtualLsn = lastWalStart + messageNo;

                            if (virtualLsn <= lastVirtualLsn && _options.UseDeduplication)
                            {
                                // already processed
                                _logger?.LogWarning("Skipping already published message: " +
                                    "WalStart = {WalStart}, " +
                                    "MessageNo = {MesageNo}, " +
                                    "LastVirtualLsn = {LastVirtualLsn}", lastWalStart, messageNo, lastVirtualLsn);
                                continue;
                            }

                            lastVirtualLsn = virtualLsn;

                            replicationMessage.Message = message;

                            var jsonMessage = await _writer.WriteMessageAsync(replicationMessage, lastVirtualLsn, cancellationToken)
                                .ConfigureAwait(false);

                            replicationMessage.HasRelationChanged = false;

                            await state.MessagePublisher.PublishAsync(jsonMessage, cancellationToken)
                                .ConfigureAwait(false);

                            // set the lastWalEnd to be sent in status update only after the message was published
                            state.LastWalEnd = message.WalEnd;

                            MetricsHelper.IncrementPublishCounter();

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
                    if (ex is PostgresException pgEx && pgEx.SqlState == "55006")
                    {
                        _logger.SafeLogWarn("Slot taken - waiting for 10 seconds...");
                    }
                    else
                    {
                        MetricsHelper.IncrementErrorCounter();
                        _logger.SafeLogError(ex, "Error in replication listener. Waiting for 10 seconds...");
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

                await DelayAsync(10_000, cancellationToken)
                    .ConfigureAwait(false);
            }

            _logger.SafeLogInfo("Disconnected from PostgreSQL");
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
                    _logger.SafeLogError(ex, "Error confirming published messages. Waiting for 10 seconds...");

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

            try
            {
                if (cs.CancellationToken.IsCancellationRequested)
                {
                    return;
                }

                await using var conn = new NpgsqlConnection(_options.ConnectionString);
                await conn.OpenAsync(cs.CancellationToken).ConfigureAwait(false);

                await using var cmd = new NpgsqlCommand(
                    "SELECT pg_logical_emit_message(false, 'keepalive', '')", conn);

                await cmd.ExecuteNonQueryAsync(cs.CancellationToken).ConfigureAwait(false);

                _logger.SafeLogDebug("Emitted idle WAL keepalive message");

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
                _logger.SafeLogError(ex, "Error emitting idle WAL message");
            }
        }
#pragma warning restore VSTHRD100 // Avoid async void methods

        private async Task DelayAsync(int time, CancellationToken cancellationToken)
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
