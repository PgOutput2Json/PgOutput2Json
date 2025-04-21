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
        private readonly IMessageWriter _writer;

        private readonly IMessagePublisherFactory _messagePublisherFactory;

        private readonly AsyncLock _lock = new AsyncLock();

        public ReplicationListener(IMessagePublisherFactory messagePublisherFactory,
                                   ReplicationListenerOptions options,
                                   JsonOptions jsonOptions,
                                   ILoggerFactory? loggerFactory)
        {
            _messagePublisherFactory = messagePublisherFactory;
            _options = options;

            _writer = jsonOptions.UseOldFormat
                ? new MessageWriterOld(jsonOptions, options)
                : new MessageWriter(jsonOptions, options);

            _loggerFactory = loggerFactory;
            _logger = loggerFactory?.CreateLogger<ReplicationListener>();
        }

        public async Task ListenForChanges(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var messagePublisher = _messagePublisherFactory.CreateMessagePublisher(_options.BatchSize, _loggerFactory);
                try
                {
                    if (_loggerFactory != null) Npgsql.NpgsqlLoggingConfiguration.InitializeLogging(_loggerFactory);

                    var _lastWalEnd = new NpgsqlLogSequenceNumber(await messagePublisher.GetLastPublishedWalSeq(cancellationToken));

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

                        var replicationOptions = new PgOutputReplicationOptions(_options.PublicationNames, PgOutputProtocolVersion.V1);

                        DateTime commitTimeStamp = DateTime.UtcNow;

                        // we will use cts to cancel the loop, if the idle confirm fails
                        using var cts = new CancellationTokenSource();

                        var unconfirmedCount = 0;

                        using var idleConfirmTimer = new Timer(async (_) =>
                        {
                            try
                            {
                                using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false))
                                {
                                    if (unconfirmedCount > 0)
                                    {
                                        await messagePublisher.ConfirmAsync(cancellationToken)
                                            .ConfigureAwait(false);
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

                                // if force confirm fails, stop the replication loop, and dispose the publisher
                                cts.TryCancel(_logger);
                            }
                        });

                        // linkedCts.Token is used only in this foreach loop,
                        // since lock ensures idle confirm cannot happen at the same time
                        var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, cts.Token);

                        await foreach (var message in connection.StartReplication(slot, replicationOptions, linkedCts.Token)
                            .ConfigureAwait(false))
                        {
                            using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false))
                            {
                                idleConfirmTimer.Change(_options.IdleFlushTime, Timeout.InfiniteTimeSpan);

                                if (message is RelationMessage rel)
                                {
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

                                if (message.WalEnd <= _lastWalEnd)
                                {
                                    // already published
                                    continue;
                                }

                                _lastWalEnd = message.WalEnd;

                                var result = await _writer.WriteMessage(message, commitTimeStamp, cancellationToken)
                                    .ConfigureAwait(false);

                                if (result.Partition < 0)
                                {
                                    continue;
                                }

                                if (_options.PartitionFilter != null
                                    && (result.Partition < _options.PartitionFilter.FromInclusive || result.Partition >= _options.PartitionFilter.ToExclusive))
                                {
                                    continue;
                                }
                                
                                await messagePublisher.PublishAsync((ulong)message.WalEnd, result.Json, result.TableNames, result.KeyKolValue, result.Partition, cancellationToken)
                                    .ConfigureAwait(false);

                                MetricsHelper.IncrementPublishCounter();

                                if (++unconfirmedCount < _options.BatchSize)
                                {
                                    continue;
                                }

                                idleConfirmTimer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);

                                await messagePublisher.ConfirmAsync(cancellationToken)
                                    .ConfigureAwait(false);

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
                    await messagePublisher.TryDisposeAsync(_logger)
                        .ConfigureAwait(false);
                }

                await Delay(10_000, cancellationToken)
                    .ConfigureAwait(false);
            }

            _logger.SafeLogInfo("Disconnected from PostgreSQL");
        }

        private async Task Delay(int time, CancellationToken cancellationToken)
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
