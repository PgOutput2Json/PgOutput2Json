using System;
using System.Collections.Generic;
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
        private class WalAwaiter
        {
            public TaskCompletionSource Source { get; set; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
            public NpgsqlLogSequenceNumber ExpectedWalEnd { get; set; }
        }

        private readonly ILoggerFactory? _loggerFactory;
        private readonly ILogger<ReplicationListener>? _logger;

        private readonly ReplicationListenerOptions _options;
        private readonly JsonOptions _jsonOptions;

        private readonly JsonWriter _writer;

        private readonly IMessagePublisherFactory _messagePublisherFactory;

        private readonly AsyncLock _lock = new AsyncLock();

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

        /*
        public async Task WhenLsnReachesAsync(string expectedLsn, CancellationToken cancellationToken)
        {
            WalAwaiter awaiter;
            CancellationTokenRegistration registration;

            var expectedWal = NpgsqlLogSequenceNumber.Parse(expectedLsn);

            using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false))
            {
                if (_confirmedWal.HasValue && _confirmedWal.Value >= expectedWal)
                {
                    return;
                }

                awaiter = new WalAwaiter {  ExpectedWalEnd = expectedWal };

                registration = cancellationToken.Register(() => awaiter.Source.TrySetCanceled(cancellationToken));

                _walAwaiters.Add(awaiter);
            }

            try
            {
#pragma warning disable VSTHRD003 // Avoid awaiting foreign Tasks

                await awaiter.Source.Task.ConfigureAwait(false);

#pragma warning restore VSTHRD003 // Avoid awaiting foreign Tasks
            }
            finally
            {
                await registration.DisposeAsync().ConfigureAwait(false);

                using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false))
                {
                    _walAwaiters.Remove(awaiter);
                }
            }
        }

        /// <summary>
        /// This is always called from inside async lock, so it's ok to iterate through awaiters
        /// </summary>
        /// <param name="walEnd"></param>
        private void SetConfirmedWal(NpgsqlLogSequenceNumber walEnd)
        {
            _confirmedWal = walEnd;

            foreach (var awaiter in _walAwaiters)
            {
                if (_confirmedWal.Value >= awaiter.ExpectedWalEnd)
                {
                    awaiter.Source.TrySetResult();
                }
            }
        }
        */

        public async Task ListenForChangesAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                IMessagePublisher? messagePublisher = null;
                CancellationTokenSource? linkedCts = null;
                Timer? idleConfirmTimer = null;

                try
                {
                    var connection = new LogicalReplicationConnection(_options.ConnectionString);

                    await using (connection.ConfigureAwait(false))
                    {
                        connection.WalReceiverStatusInterval = Timeout.InfiniteTimeSpan; // we are sending status manually

                        await connection.Open(cancellationToken)
                            .ConfigureAwait(false);

                        _logger.SafeLogInfo("Connected to PostgreSQL");

                        PgOutputReplicationSlot slot;

                        if (!_options.UseTemporarySlot)
                        {
                            slot = new PgOutputReplicationSlot(_options.ReplicationSlotName);
                        }
                        else
                        {
                            var slotName = string.IsNullOrWhiteSpace(_options.ReplicationSlotName)
                                ? $"pg2j_{Guid.NewGuid().ToString().Replace("-", "")}"
                                : _options.ReplicationSlotName;

                            slot = await connection.CreatePgOutputReplicationSlot(slotName, true, cancellationToken: cancellationToken)
                                .ConfigureAwait(false);
                        }

                        // start data export after creating the temporary replication slot
                        await DataExporter.MaybeExportDataAsync(_messagePublisherFactory, _options, _jsonOptions, _loggerFactory, cancellationToken).ConfigureAwait(false);

                        messagePublisher = _messagePublisherFactory.CreateMessagePublisher(_options, _loggerFactory);

                        // virtual lsn is start lsn + msg number
                        var lastVirtualLsn = new NpgsqlLogSequenceNumber(await messagePublisher.GetLastPublishedWalSeqAsync(cancellationToken).ConfigureAwait(false));

                        var lastWalStart = new NpgsqlLogSequenceNumber(0);
                        
                        var lastWalEnd = new NpgsqlLogSequenceNumber(0);

                        // this counts messages with the same WalStart
                        var messageNo = 0UL;

                        var replicationOptions = new PgOutputReplicationOptions(_options.PublicationNames, PgOutputProtocolVersion.V1);

                        // we will use cts to cancel the loop, if the idle confirm fails
                        linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

                        var unconfirmedCount = 0;

                        async Task TimerCallbackAsync()
                        {
                            try
                            {
                                if (cancellationToken.IsCancellationRequested)
                                {
                                    return;
                                }

                                using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false))
                                {
                                    if (unconfirmedCount > 0 && messagePublisher != null)
                                    {
                                        await messagePublisher.ConfirmAsync(cancellationToken)
                                            .ConfigureAwait(false);
                                    }

                                    unconfirmedCount = 0;

                                    await SendStatusUpdateAsync(connection, lastWalEnd, cancellationToken)
                                        .ConfigureAwait(false);

                                    _logger.SafeLogDebug("Idle Confirmed PostgreSQL");
                                }
                            }
                            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                            {
                                // stopping - nothing to do
                            }
                            catch (Exception ex)
                            {
                                MetricsHelper.IncrementErrorCounter();
                                _logger.SafeLogError(ex, "Error confirming published messages. Waiting for 10 seconds...");

                                try
                                {
                                    using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false))
                                    {
                                        if (linkedCts != null)
                                        {
                                            // if force confirm fails, stop the replication loop, and dispose the publisher
                                            await linkedCts.CancelAsync().ConfigureAwait(false);
                                        }
                                    }
                                }
                                catch (Exception exx)
                                {
                                    MetricsHelper.IncrementErrorCounter();
                                    _logger.SafeLogError(exx, "Error cancelling link token source");
                                }
                            }
                        }

                        idleConfirmTimer = new Timer(_ =>
                        {
                            _ = TimerCallbackAsync(); // fire and forget
                        });

                        var replicationMessage = new ReplicationMessage { HasRelationChanged = true, CommitTimeStamp = DateTime.UtcNow, };

                        // linkedCts.Token is used only in this foreach loop,
                        // since lock ensures idle confirm cannot happen at the same time
                        await foreach (var message in connection.StartReplication(slot, replicationOptions, linkedCts.Token)
                            .ConfigureAwait(false))
                        {
                            using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false))
                            {
                                 //_logger?.LogWarning("{Type} {WalStart}/{MesssageNo}", message.GetType().Name, message.WalStart, messageNo);

                                idleConfirmTimer.Change(_options.BatchWaitTime, Timeout.InfiniteTimeSpan);

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
                                        "LastVirtualLsn = {LastVirtualLsn}" +
                                        lastWalStart, messageNo, lastVirtualLsn);
                                    continue;
                                }

                                lastVirtualLsn = virtualLsn;

                                replicationMessage.Message = message;

                                var jsonMessage = await _writer.WriteMessageAsync(replicationMessage, lastVirtualLsn, cancellationToken)
                                    .ConfigureAwait(false);

                                replicationMessage.HasRelationChanged = false;

                                if (jsonMessage.Partition < 0)
                                {
                                    lastWalEnd = message.WalEnd; // skipped message, treat as published
                                    continue;
                                }

                                if (_options.PartitionFilter != null
                                    && (jsonMessage.Partition < _options.PartitionFilter.FromInclusive || jsonMessage.Partition >= _options.PartitionFilter.ToExclusive))
                                {
                                    lastWalEnd = message.WalEnd; // skipped message, treat as published
                                    continue;
                                }

                                await messagePublisher.PublishAsync(jsonMessage, cancellationToken)
                                    .ConfigureAwait(false);

                                // set the lastWalEnd to be sent in status update only after the message was published
                                lastWalEnd = message.WalEnd;

                                MetricsHelper.IncrementPublishCounter();

                                if (++unconfirmedCount < _options.BatchSize)
                                {
                                    continue;
                                }

                                idleConfirmTimer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);

                                await messagePublisher.ConfirmAsync(cancellationToken)
                                    .ConfigureAwait(false);

                                unconfirmedCount = 0;

                                await SendStatusUpdateAsync(connection, lastWalEnd, cancellationToken)
                                        .ConfigureAwait(false);

                                _logger.SafeLogDebug("Confirmed PostgreSQL");
                            }
                        }
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    _logger.SafeLogWarn("Stopping ReplicationListener - cancellation requested");
                    break;
                }
                catch (OperationCanceledException) when (linkedCts != null && linkedCts.IsCancellationRequested)
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
                    await idleConfirmTimer.TryDisposeAsync(_logger).ConfigureAwait(false);
                    idleConfirmTimer = null;

                    // we don't use cancellation token here, as we want to dispose always
                    using (await _lock.LockAsync(CancellationToken.None).ConfigureAwait(false))
                    {
                        await messagePublisher.TryDisposeAsync(_logger).ConfigureAwait(false);
                        messagePublisher = null;

                        linkedCts.TryDispose(_logger);
                        linkedCts = null;
                    }
                }

                await DelayAsync(10_000, cancellationToken)
                    .ConfigureAwait(false);
            }

            _logger.SafeLogInfo("Disconnected from PostgreSQL");
        }

        private static async Task SendStatusUpdateAsync(LogicalReplicationConnection connection, NpgsqlLogSequenceNumber lastWalEnd, CancellationToken cancellationToken)
        {
            if ((ulong)lastWalEnd > 0)
            {
                connection.SetReplicationStatus(lastWalEnd);

                await connection.SendStatusUpdate(cancellationToken)
                    .ConfigureAwait(false);
            }
        }

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
