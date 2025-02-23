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
        private readonly MessageWriter _writer;

        private readonly IMessagePublisherFactory _messagePublisherFactory;

        private readonly AsyncLock _lock = new AsyncLock();

        public ReplicationListener(IMessagePublisherFactory messagePublisherFactory,
                                   ReplicationListenerOptions options,
                                   JsonOptions jsonOptions,
                                   ILoggerFactory? loggerFactory)
        {
            _messagePublisherFactory = messagePublisherFactory;
            _options = options;

            _writer = new MessageWriter(jsonOptions, options);

            _loggerFactory = loggerFactory;
            _logger = loggerFactory?.CreateLogger<ReplicationListener>();
        }

        public async Task ListenForChanges(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var messagePublisher = _messagePublisherFactory.CreateMessagePublisher(_loggerFactory);
                try
                {
                    var connection = new LogicalReplicationConnection(_options.ConnectionString);
                    await using (connection.ConfigureAwait(false))
                    {
                        connection.WalReceiverStatusInterval = TimeSpan.FromSeconds(5);

                        await connection.Open(cancellationToken)
                            .ConfigureAwait(false);

                        _logger.SafeLogInfo("Connected to PostgreSQL");

                        PgOutputReplicationSlot slot;

                        if (_options.ReplicationSlotName != string.Empty)
                        {
                            slot = new PgOutputReplicationSlot(_options.ReplicationSlotName);
                        }
                        else
                        {
                            var slotName = $"pg2j_{Guid.NewGuid().ToString().Replace("-", "")}";
                            slot = await connection.CreatePgOutputReplicationSlot(slotName, true, cancellationToken: cancellationToken)
                                .ConfigureAwait(false);
                        }

                        var replicationOptions = new PgOutputReplicationOptions(_options.PublicationNames, 1);

                        DateTime commitTimeStamp = DateTime.UtcNow;

                        NpgsqlLogSequenceNumber? lastWalEnd = null;

                        using var cts = new CancellationTokenSource();
                        var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, cts.Token);

                        using var forceConfirmTimer = new Timer(async (_) =>
                        {
                            try
                            {
                                using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false))
                                {
                                    if (!lastWalEnd.HasValue) return;

                                    await messagePublisher.ForceConfirmAsync(cancellationToken)
                                        .ConfigureAwait(false);

                                    connection.SetReplicationStatus(lastWalEnd.Value);
                                    lastWalEnd = null;
                                }
                            }
                            catch (Exception ex)
                            {
                                _logger.SafeLogError(ex, "Error confirming published messages. Waiting for 10 seconds...");

                                // if force confirm fails, stop the replication loop, and dispose the publisher
                                cts.Cancel();
                            }
                        });

                        await foreach (var message in connection.StartReplication(slot, replicationOptions, linkedCts.Token)
                            .ConfigureAwait(false))
                        {
                            using (await _lock.LockAsync(linkedCts.Token).ConfigureAwait(false))
                            {
                                lastWalEnd = message.WalEnd;

                                if (message is BeginMessage beginMsg)
                                {
                                    commitTimeStamp = beginMsg.TransactionCommitTimestamp;
                                    continue;
                                }

                                var result = await _writer.WriteMessage(message, commitTimeStamp, linkedCts.Token)
                                    .ConfigureAwait(false);

                                if (result.Partition < 0)
                                {
                                    continue;
                                }

                                var confirm = await messagePublisher.PublishAsync(result.Json, result.TableNames, result.KeyKolValue, result.Partition, linkedCts.Token)
                                    .ConfigureAwait(false);

                                if (confirm)
                                {
                                    forceConfirmTimer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);

                                    connection.SetReplicationStatus(message.WalEnd);
                                    lastWalEnd = null;

                                    _logger.SafeLogInfo("Confirmed PostgreSQL");
                                }
                                else
                                {
                                    forceConfirmTimer.Change(TimeSpan.FromSeconds(10), Timeout.InfiniteTimeSpan);
                                }
                            }
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    if (ex.Message.StartsWith("55006:"))
                    {
                        _logger.SafeLogWarn("Slot taken - waiting for 10 seconds...");
                    }
                    else
                    {
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
