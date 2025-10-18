using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;

namespace PgOutput2Json.RabbitMqStreams
{
    public class RabbitMqStreamsPublisher: MessagePublisher
    {
        public RabbitMqStreamsPublisher(RabbitMqStreamsPublisherOptions options, int batchSize, ILoggerFactory? loggerFactory = null)
        {
            _options = options;
            _batchSize = batchSize;
            
            _loggerStreamSystem = loggerFactory?.CreateLogger<StreamSystem>();
            _loggerProducer = loggerFactory?.CreateLogger<Producer>();
            _logger = loggerFactory?.CreateLogger<RabbitMqStreamsPublisher>();
        }

        public async override Task PublishAsync(JsonMessage jsonMsg, CancellationToken token)
        {
            var producer = await EnsureProducerAsync().ConfigureAwait(false);

            lock (_confirmationLock)
            {
                _unconfirmedCount++;

                if (_confirmationTaskCompletionSource == null || _confirmationTaskCompletionSource.Task.IsCompleted)
                {
                    _confirmationTaskCompletionSource = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                }
            }

            var json = jsonMsg.Json.ToString();

            var msg = new Message(Encoding.UTF8.GetBytes(json))
            {
                Properties = new RabbitMQ.Stream.Client.AMQP.Properties
                {
                    MessageId = _options.WriteTableNameToMessageKey
                        ? string.Join("", jsonMsg.TableName.ToString(), jsonMsg.KeyKolValue.ToString())
                        : jsonMsg.KeyKolValue.ToString(),
                },
            };

            if (jsonMsg.PartitionKolValue.Length > 0)
            {
                msg.ApplicationProperties = new RabbitMQ.Stream.Client.AMQP.ApplicationProperties
                {
                    { ApplicationProperties.PartitionKey, jsonMsg.PartitionKolValue.ToString() }
                };
            }

            await producer.Send(msg).ConfigureAwait(false);

            if (_logger != null && _logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug("Published to {StreamName}: {Body}", _options.StreamName, json);
            }
        }

        public async override Task ConfirmAsync(CancellationToken token)
        {
            // only PublishAsync can create the source, ConfirmAsync is never called in parallel
            if (_confirmationTaskCompletionSource != null)
            {
                using (token.Register(() => _confirmationTaskCompletionSource.TrySetCanceled(token)))
                {
#pragma warning disable VSTHRD003 // Avoid awaiting foreign Tasks

                    await _confirmationTaskCompletionSource.Task.ConfigureAwait(false);

#pragma warning restore VSTHRD003 // Avoid awaiting foreign Tasks
                }
            }
        }

        public override async ValueTask DisposeAsync()
        {
            if (_producer != null)
            {
                try
                {
                    await _producer.Close()
                        .ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Error closing RabbitMq StreamSystem");
                }
            }

            if (_streamSystem != null && !_streamSystem.IsClosed)
            {
                try
                {
                    await _streamSystem.Close()
                        .ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Error closing RabbitMq StreamSystem");
                }
            }

            _producer = null;
            _streamSystem = null;
        }

        private async Task<StreamSystem> EnsureStreamSystemAsync()
        {
            if (_streamSystem != null && !_streamSystem.IsClosed) return _streamSystem;

            _logger?.LogInformation("Creating stream system");

            _streamSystem = await StreamSystem.Create(_options.StreamSystemConfig, _loggerStreamSystem)
                .ConfigureAwait(false);

            _logger?.LogInformation("Created stream system");

            return _streamSystem;
        }

        private async Task<Producer> EnsureProducerAsync()
        {
            if (_producer != null && _producer.IsOpen()) return _producer;
            
            var streamSystem = await EnsureStreamSystemAsync().ConfigureAwait(false);

            _logger?.LogInformation("Creating producer for: {StreamName}", _options.StreamName);

            _producer = await Producer.Create(
                new ProducerConfig(streamSystem, _options.StreamName)
                {
                    SuperStreamConfig = _options.IsSuperStream ? _options.SuperStreamConfig : null,
                    MaxInFlight = _batchSize,
                    MessagesBufferSize = _options.MessageBufferSize,
                    ClientProvidedName = $"{_options.StreamSystemConfig.ClientProvidedName}-producer",
                    ConfirmationHandler = confirmation =>
                    {
                        switch (confirmation.Status)
                        {
                            case ConfirmationStatus.Confirmed:
                                lock (_confirmationLock)
                                {
                                    _unconfirmedCount--;
                                    if (_unconfirmedCount == 0)
                                    {
                                        _confirmationTaskCompletionSource?.SetResult();
                                    }
                                }

                                break;

                            case ConfirmationStatus.StreamNotAvailable:
                            case ConfirmationStatus.InternalError:
                            case ConfirmationStatus.AccessRefused:
                            case ConfirmationStatus.PreconditionFailed:
                            case ConfirmationStatus.PublisherDoesNotExist:
                            case ConfirmationStatus.UndefinedError:
                            case ConfirmationStatus.ClientTimeoutError:
                                lock (_confirmationLock)
                                {
                                    _confirmationTaskCompletionSource?.SetException(new Exception(
                                        $"Stream {confirmation.Stream} Message {confirmation.PublishingId} failed with {confirmation.Status}"));
                                }
                                break;
                            default:
                                throw new ArgumentOutOfRangeException();
                        }

                        return Task.CompletedTask;
                    },
                },
                _loggerProducer
            )
            .ConfigureAwait(false);

            _logger?.LogInformation("Created producer for: {StreamName}", _options.StreamName);

            return _producer;
        }

        public override async Task<ulong> GetLastPublishedWalSeqAsync(CancellationToken stoppingToken)
        {
            _logger?.LogInformation("Reading last published WAL LSN for: {StreamName}", _options.StreamName);

            var system = await EnsureStreamSystemAsync().ConfigureAwait(false);

            var partitions = _options.IsSuperStream
                    ? await system.QueryPartition(_options.StreamName).ConfigureAwait(false)
                    : [_options.StreamName];

            var maxWalEnd = 0UL;

            foreach (var partition in partitions)
            {
                try
                {
                    var stats = await system.StreamStats(partition).ConfigureAwait(false);
                    var firstOffset = stats.CommittedChunkId();
                }
                catch (Exception ex)
                {
                    _logger?.LogInformation("Empty stream detected: {StreamName} ({ErrorMessage}).", partition, ex.Message);
                    continue;
                }

                string? json = null;

                var firstMessageReceived = new TaskCompletionSource<bool>();

                // make sure we cancel the wait, if we're stopping
                var registration = stoppingToken.Register(() => firstMessageReceived.TrySetCanceled());

                var consumer = await Consumer.Create(new ConsumerConfig(system, partition)
                {
                    OffsetSpec = new OffsetTypeLast(),
                    ClientProvidedName = $"{_options.StreamSystemConfig.ClientProvidedName}-consumer",
                    MessageHandler = (stream, consumer, context, message) =>
                    {
                        // Keep overwriting so we end up with the last message
                        json = Encoding.UTF8.GetString(message.Data.Contents);
                        firstMessageReceived.TrySetResult(true);
                        return Task.CompletedTask;
                    }
                }).ConfigureAwait(false);

                try
                {
                    // Wait for the first message (guaranteed to exist)
                    await firstMessageReceived.Task.ConfigureAwait(false);

                    // Wait 200ms for any remaining messages in the chunk
                    await Task.Delay(200, stoppingToken).ConfigureAwait(false);
                }
                finally
                {
                    await consumer.Close().ConfigureAwait(false);
                    await registration.DisposeAsync().ConfigureAwait(false);
                }

                if (json == null)
                {
                    throw new Exception($"Cannot read last WAL end LSN. No messages read from an non-empty stream.");
                }

                if (!json.TryGetWalEnd(out var walEnd))
                {
                    throw new Exception($"Missing WAL end LSN in the message: '{json}'");
                }

                _logger?.LogInformation("Last published WAL LSN for {Stream}: {LastWalSeq}", partition, walEnd);

                if (walEnd > maxWalEnd)
                {
                    maxWalEnd = walEnd;
                }
            }

            _logger?.LogInformation("Last published WAL LSN for {Stream}: {LastWalSeq}", _options.StreamName, maxWalEnd);
            
            return maxWalEnd;
        }

        private StreamSystem? _streamSystem;
        private Producer? _producer;

        private int _unconfirmedCount = 0;
        private TaskCompletionSource? _confirmationTaskCompletionSource;

        private readonly object _confirmationLock = new();

        private readonly RabbitMqStreamsPublisherOptions _options;
        private readonly int _batchSize;
        private readonly ILogger<StreamSystem>? _loggerStreamSystem;
        private readonly ILogger<Producer>? _loggerProducer;
        private readonly ILogger<RabbitMqStreamsPublisher>? _logger;
    }
}