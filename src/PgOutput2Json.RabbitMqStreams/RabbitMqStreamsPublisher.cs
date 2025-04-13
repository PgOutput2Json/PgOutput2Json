using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;

namespace PgOutput2Json.RabbitMqStreams
{
    public class RabbitMqStreamsPublisher: IMessagePublisher
    {
        public RabbitMqStreamsPublisher(RabbitMqStreamsPublisherOptions options, int batchSize, ILoggerFactory? loggerFactory = null)
        {
            _options = options;
            _batchSize = batchSize;
            
            _loggerStreamSystem = loggerFactory?.CreateLogger<StreamSystem>();
            _loggerProducer = loggerFactory?.CreateLogger<Producer>();
            _logger = loggerFactory?.CreateLogger<RabbitMqStreamsPublisher>();
        }

        public async Task PublishAsync(ulong walSeqNo, string json, string tableName, string keyColumnValue, int partition, CancellationToken token)
        {
            if (_lastWalSeq == 0 && _options.UseDeduplication)
            {
                _lastWalSeq = await GetLastPublishedWalSeq();
            }

            if (walSeqNo <= _lastWalSeq)
            {
                return;
            }

            _lastWalSeq = walSeqNo;

            var producer = await EnsureProducer();

            lock (_confirmationLock)
            {
                _unconfirmedCount++;

                if (_confirmationTaskCompletionSource == null || _confirmationTaskCompletionSource.Task.IsCompleted)
                {
                    _confirmationTaskCompletionSource = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                }
            }

            await producer.Send(new Message(Encoding.UTF8.GetBytes(json)))
                .ConfigureAwait(false);
        }

        public async Task ConfirmAsync(CancellationToken token)
        {
            // only PublishAsync can create the source, ConfirmAsync is never called in parallel
            if (_confirmationTaskCompletionSource != null)
            {
                using (token.Register(() => _confirmationTaskCompletionSource.TrySetCanceled(token)))
                {
                    await _confirmationTaskCompletionSource.Task
                        .ConfigureAwait(false);
                }
            }
        }

        public virtual async ValueTask DisposeAsync()
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

        private async Task<StreamSystem> EnsureStreamSystem()
        {
            if (_streamSystem != null && !_streamSystem.IsClosed) return _streamSystem;

            _logger?.LogInformation("Creating to Stream System");

            _streamSystem = await StreamSystem.Create(_options.StreamSystemConfig, _loggerStreamSystem)
                .ConfigureAwait(false);

            return _streamSystem;
        }

        private async Task<Producer> EnsureProducer()
        {
            if (_producer != null && _producer.IsOpen()) return _producer;
            
            var streamSystem = await EnsureStreamSystem().ConfigureAwait(false);

            _logger?.LogInformation("Creating to Producer for: {StreamName}", _options.StreamName);

            _producer = await Producer.Create(
                new ProducerConfig(streamSystem, _options.StreamName)
                {
                    ReconnectStrategy = new NoReconnectStrategy(),
                    ResourceAvailableReconnectStrategy = new NoReconnectStrategy(),
                    MaxInFlight = _batchSize,
                    MessagesBufferSize = _options.UseDeduplication ? 1 : _batchSize,
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

            _logger?.LogInformation("Created to Producer for: {StreamName}", _options.StreamName);

            return _producer;
        }

        private async Task<ulong> GetLastPublishedWalSeq()
        {
            _logger?.LogInformation("Reading last published WAL LSN for: {StreamName}", _options.StreamName);

            var streamSystem = await EnsureStreamSystem();

            using var cancellationTokenSource = new CancellationTokenSource();

            string? json = null;

            var consumer = await Consumer.Create(new ConsumerConfig(streamSystem, _options.StreamName)
            {
                OffsetSpec = new OffsetTypeLast(),
                ClientProvidedName = $"{_options.StreamSystemConfig.ClientProvidedName}-consumer",
                ReconnectStrategy = new NoReconnectStrategy(),
                ResourceAvailableReconnectStrategy = new NoReconnectStrategy(),
                MessageHandler = (stream, consumer, context, message) =>
                {
                    json = Encoding.UTF8.GetString(message.Data.Contents);
                    cancellationTokenSource.Cancel();

                    return Task.CompletedTask;
                }
            }).ConfigureAwait(false);


            try
            {
                await Task.Delay(TimeSpan.FromSeconds(10), cancellationTokenSource.Token)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
            
            await consumer.Close()
                .ConfigureAwait(false);

            if (json != null)
            {
                using JsonDocument doc = JsonDocument.Parse(json);

                if (doc.RootElement.TryGetProperty("_we", out JsonElement weProperty)
                    && weProperty.ValueKind == JsonValueKind.Number
                    && weProperty.TryGetUInt64(out var value))
                {
                    return value;
                }

                throw new Exception($"Missing WAL end LSN in the message: '{json}'");
            }

            _logger?.LogWarning("Timeout consuming the last message to ensure deduplication. Possibly empty stream: {StreamName}", _options.StreamName);
            return 0;
        }

        private StreamSystem? _streamSystem;
        private Producer? _producer;

        private int _unconfirmedCount = 0;
        private TaskCompletionSource? _confirmationTaskCompletionSource;

        private ulong _lastWalSeq = 0;

        private readonly object _confirmationLock = new();

        private readonly RabbitMqStreamsPublisherOptions _options;
        private readonly int _batchSize;
        private readonly ILogger<StreamSystem>? _loggerStreamSystem;
        private readonly ILogger<Producer>? _loggerProducer;
        private readonly ILogger<RabbitMqStreamsPublisher>? _logger;

        private class NoReconnectStrategy : IReconnectStrategy
        {
            public ValueTask WhenConnected(string itemIdentifier)
            {
                return ValueTask.CompletedTask;
            }

            public ValueTask<bool> WhenDisconnected(string itemIdentifier)
            {
                return ValueTask.FromResult(false);
            }
        }
    }
}