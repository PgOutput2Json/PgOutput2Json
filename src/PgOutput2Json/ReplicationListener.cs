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

        private readonly IMessageWriter _writer;

        private readonly IMessagePublisherFactory _messagePublisherFactory;

        private readonly AsyncLock _lock = new AsyncLock();

        private readonly List<WalAwaiter> _walAwaiters = [];

        private NpgsqlLogSequenceNumber? _committedWal;

        public ReplicationListener(IMessagePublisherFactory messagePublisherFactory,
                                   ReplicationListenerOptions options,
                                   JsonOptions jsonOptions,
                                   ILoggerFactory? loggerFactory)
        {
            _messagePublisherFactory = messagePublisherFactory;
            _options = options;
            _jsonOptions = jsonOptions;
            _writer = jsonOptions.UseOldFormat
                ? new MessageWriterOld(jsonOptions, options)
                : new MessageWriter(jsonOptions, options);

            _loggerFactory = loggerFactory;
            _logger = loggerFactory?.CreateLogger<ReplicationListener>();
        }

        public async Task WhenLsnReachesAsync(string expectedLsn, CancellationToken cancellationToken)
        {
            WalAwaiter awaiter;
            CancellationTokenRegistration registration;

            var expectedWal = NpgsqlLogSequenceNumber.Parse(expectedLsn);

            using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false))
            {
                if (_committedWal.HasValue && _committedWal.Value >= expectedWal)
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
        private void SetCommittedWal(NpgsqlLogSequenceNumber walEnd)
        {
            _committedWal = walEnd;

            foreach (var awaiter in _walAwaiters)
            {
                if (_committedWal.Value >= awaiter.ExpectedWalEnd)
                {
                    awaiter.Source.TrySetResult();
                }
            }
        }

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

                        var lastWalEnd = new NpgsqlLogSequenceNumber(await messagePublisher.GetLastPublishedWalSeqAsync(cancellationToken).ConfigureAwait(false));

                        using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false))
                        {
                            SetCommittedWal(lastWalEnd);
                        }

                        var replicationOptions = new PgOutputReplicationOptions(_options.PublicationNames, PgOutputProtocolVersion.V1);

                        DateTime commitTimeStamp = DateTime.UtcNow;

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

                                        SetCommittedWal(lastWalEnd);
                                    }

                                    unconfirmedCount = 0;

                                    await connection.SendStatusUpdate(cancellationToken)
                                        .ConfigureAwait(false);

                                    _logger.SafeLogDebug("Idle Confirmed PostgreSQL");
                                }
                            }
                            catch (OperationCanceledException)
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

                        var hasRelationChanged = true;

                        // linkedCts.Token is used only in this foreach loop,
                        // since lock ensures idle confirm cannot happen at the same time
                        await foreach (var message in connection.StartReplication(slot, replicationOptions, linkedCts.Token)
                            .ConfigureAwait(false))
                        {
                            using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false))
                            {
                                idleConfirmTimer.Change(TimeSpan.FromMilliseconds(250), Timeout.InfiniteTimeSpan);

                                if (message is RelationMessage rel)
                                {
                                    hasRelationChanged = true;

                                    // Relation Message has WalEnd=0/0
                                    continue;
                                }

                                if (message is BeginMessage beginMsg)
                                {
                                    commitTimeStamp = beginMsg.TransactionCommitTimestamp;
                                    continue;
                                }
                                else if (message is CommitMessage commitMsg)
                                {
                                    // replication status only make sense on commit (it seems)
                                    connection.SetReplicationStatus(message.WalEnd);
                                    continue;
                                }

                                if (message.WalEnd <= lastWalEnd)
                                {
                                    // already published
                                    continue;
                                }

                                lastWalEnd = message.WalEnd;

                                var result = await _writer.WriteMessageAsync(message, commitTimeStamp, hasRelationChanged, cancellationToken)
                                    .ConfigureAwait(false);

                                if (hasRelationChanged && _options.CopyData)
                                {
                                    // if data copy is on, we will mark this table as copied,
                                    // to avoid copying tables created after the inital copy
                                    await DataCopyProgress.SetDataCopyProgressAsync(result.TableName, _options, true, null, null, cancellationToken)
                                        .ConfigureAwait(false);
                                }

                                hasRelationChanged = false;

                                if (result.Partition < 0)
                                {
                                    continue;
                                }

                                if (_options.PartitionFilter != null
                                    && (result.Partition < _options.PartitionFilter.FromInclusive || result.Partition >= _options.PartitionFilter.ToExclusive))
                                {
                                    continue;
                                }

                                await messagePublisher.PublishAsync((ulong)message.WalEnd, result.Json, result.TableName, result.KeyKolValue, result.Partition, cancellationToken)
                                    .ConfigureAwait(false);

                                MetricsHelper.IncrementPublishCounter();

                                if (++unconfirmedCount < _options.BatchSize)
                                {
                                    continue;
                                }

                                idleConfirmTimer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);

                                await messagePublisher.ConfirmAsync(cancellationToken)
                                    .ConfigureAwait(false);

                                SetCommittedWal(lastWalEnd);

                                unconfirmedCount = 0;

                                await connection.SendStatusUpdate(cancellationToken)
                                    .ConfigureAwait(false);

                                _logger.SafeLogDebug("Confirmed PostgreSQL");
                            }
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        _logger.SafeLogWarn("Stopping ReplicationListener - cancellation requested");
                        break;
                    }
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
