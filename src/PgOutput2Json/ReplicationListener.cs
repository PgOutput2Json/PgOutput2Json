using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;
using NpgsqlTypes;

namespace PgOutput2Json
{
    internal sealed class ReplicationListener
    {
        private readonly ILoggerFactory? _loggerFactory;
        private readonly ILogger<ReplicationListener>? _logger;

        private readonly ReplicationListenerOptions _options;
        private readonly JsonOptions _jsonOptions;

        private readonly JsonWriter _writer;

        private readonly IMessagePublisherFactory _messagePublisherFactory;

        private readonly AsyncLock _lock = new();

        private Timer? _idleConfirmTimer;

        private LogicalReplicationConnection? _connection;
        private IMessagePublisher? _messagePublisher;

        private CancellationToken _cancellationToken = CancellationToken.None;
        private CancellationTokenSource? _linkedCts;

        private int _unconfirmedCount;
        private NpgsqlLogSequenceNumber _lastWalEnd;

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
                Npgsql.NpgsqlLoggingConfiguration.InitializeLogging(_loggerFactory);
            }
        }

        public async Task ListenForChangesAsync(CancellationToken cancellationToken)
        {
            if (_connection != null) throw new Exception("Already listening");

            _cancellationToken = cancellationToken;

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    _connection = new LogicalReplicationConnection(_options.ConnectionString)
                    {
                        WalReceiverStatusInterval = Timeout.InfiniteTimeSpan // we are sending status manually
                    };

                    await _connection.Open(cancellationToken)
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
                        slot = await _connection.CreatePgOutputReplicationSlot(slotName, true, cancellationToken: cancellationToken)
                            .ConfigureAwait(false);
                    }

                    // start data export after creating the temporary replication slot
                    await DataExporter.MaybeExportDataAsync(_messagePublisherFactory, _options, _jsonOptions, slotName, _loggerFactory, cancellationToken).ConfigureAwait(false);

                    _messagePublisher = _messagePublisherFactory.CreateMessagePublisher(_options, slotName, _loggerFactory);

                    // virtual lsn is start lsn + msg number
                    var lastVirtualLsn = new NpgsqlLogSequenceNumber(await _messagePublisher.GetLastPublishedWalSeqAsync(cancellationToken).ConfigureAwait(false));

                    var lastWalStart = new NpgsqlLogSequenceNumber(0);

                    _lastWalEnd = new NpgsqlLogSequenceNumber(0);

                    // this counts messages with the same WalStart
                    var messageNo = 0UL;

                    var replicationOptions = new PgOutputReplicationOptions(_options.PublicationNames, PgOutputProtocolVersion.V1);

                    // we will use cts to cancel the loop, if the idle confirm fails
                    _linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

                    _unconfirmedCount = 0;

                    using (ExecutionContext.SuppressFlow())
                    {
                        _idleConfirmTimer = new Timer(IdleTimerCallback);
                    }

                    var replicationMessage = new ReplicationMessage { HasRelationChanged = true, CommitTimeStamp = DateTime.UtcNow, };

                    // linkedCts.Token is used only in this foreach loop,
                    // since lock ensures idle confirm cannot happen at the same time
                    await foreach (var message in _connection.StartReplication(slot, replicationOptions, _linkedCts.Token)
                        .ConfigureAwait(false))
                    {
                        using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false))
                        {
                            //_logger?.LogWarning("{Type} {WalStart}/{MesssageNo}", message.GetType().Name, message.WalStart, messageNo);

                            _idleConfirmTimer.Change(_options.BatchWaitTime, Timeout.InfiniteTimeSpan);

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
                                _lastWalEnd = message.WalEnd;
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

                            await _messagePublisher.PublishAsync(jsonMessage, cancellationToken)
                                .ConfigureAwait(false);

                            // set the lastWalEnd to be sent in status update only after the message was published
                            _lastWalEnd = message.WalEnd;

                            MetricsHelper.IncrementPublishCounter();

                            if (++_unconfirmedCount < _options.BatchSize)
                            {
                                continue;
                            }

                            _idleConfirmTimer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);

                            await ConfirmAsync(_connection, _messagePublisher, _unconfirmedCount, _lastWalEnd, cancellationToken).ConfigureAwait(false);

                            _unconfirmedCount = 0;

                            _logger.SafeLogDebug("Confirmed PostgreSQL");
                        }
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    _logger.SafeLogWarn("Stopping ReplicationListener - cancellation requested");
                    break;
                }
                catch (OperationCanceledException) when (_linkedCts != null && _linkedCts.IsCancellationRequested)
                {
                    // cancelled because idle confirm failed, nothing to do - error has been logged already
                }
                catch (Exception ex)
                {
                    if (ex.Message.StartsWith("55006:"))
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
                        _idleConfirmTimer.TryDispose(_logger);
                        _idleConfirmTimer = null;

                        _linkedCts.TryDispose(_logger);
                        _linkedCts = null;

                        await _messagePublisher.TryDisposeAsync(_logger).ConfigureAwait(false);
                        _messagePublisher = null;

                        await _connection.TryDisposeAsync(_logger).ConfigureAwait(false);
                        _connection = null;
                    }
                }

                await DelayAsync(10_000, cancellationToken)
                    .ConfigureAwait(false);
            }

            _logger.SafeLogInfo("Disconnected from PostgreSQL");
        }

        private static async Task ConfirmAsync(LogicalReplicationConnection connection,
                                               IMessagePublisher messagePublisher,
                                               int unconfirmedCount,
                                               NpgsqlLogSequenceNumber lastWalEnd,
                                               CancellationToken cancellationToken)
        {
            if (unconfirmedCount > 0)
            {
                await messagePublisher.ConfirmAsync(cancellationToken).ConfigureAwait(false);
            }

            if ((ulong)lastWalEnd > 0)
            {
                connection.SetReplicationStatus(lastWalEnd);

                await connection.SendStatusUpdate(cancellationToken)
                    .ConfigureAwait(false);
            }
        }

#pragma warning disable VSTHRD100 // Avoid async void methods
        private async void IdleTimerCallback(object? state)
        {
            try
            {
                if (_cancellationToken.IsCancellationRequested)
                {
                    return;
                }

                using (await _lock.LockAsync(_cancellationToken).ConfigureAwait(false))
                {
                    if (_connection != null && _messagePublisher != null)
                    {
                        await ConfirmAsync(_connection, _messagePublisher, _unconfirmedCount, _lastWalEnd, _cancellationToken).ConfigureAwait(false);

                        _unconfirmedCount = 0;
                    }

                    _logger.SafeLogDebug("Idle Confirmed PostgreSQL");
                }
            }
            catch (OperationCanceledException) when (_cancellationToken.IsCancellationRequested)
            {
                // stopping - nothing to do
            }
            catch (Exception ex)
            {
                try
                {
                    MetricsHelper.IncrementErrorCounter();
                    _logger.SafeLogError(ex, "Error confirming published messages. Waiting for 10 seconds...");

                    using (await _lock.LockAsync(_cancellationToken).ConfigureAwait(false))
                    {
                        if (_linkedCts != null)
                        {
                            // if force confirm fails, stop the replication loop, and dispose the publisher
                            await _linkedCts.CancelAsync().ConfigureAwait(false);
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
