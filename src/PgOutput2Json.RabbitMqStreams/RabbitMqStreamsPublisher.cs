using System;
using System.Text;
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

            if (_logger != null && _logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug("Published to Stream={0}, Body={1}", _options.StreamName, json);
            }
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

            _logger?.LogInformation("Creating stream system");

            _streamSystem = await StreamSystem.Create(_options.StreamSystemConfig, _loggerStreamSystem)
                .ConfigureAwait(false);

            _logger?.LogInformation("Created stream system");

            return _streamSystem;
        }

        private async Task<Producer> EnsureProducer()
        {
            if (_producer != null && _producer.IsOpen()) return _producer;
            
            var streamSystem = await EnsureStreamSystem().ConfigureAwait(false);

            _logger?.LogInformation("Creating producer for: {StreamName}", _options.StreamName);

            _producer = await Producer.Create(
                new ProducerConfig(streamSystem, _options.StreamName)
                {
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

        public async Task<ulong?> GetLastPublishedWalSeq(CancellationToken stoppingToken)
        {
            _logger?.LogInformation("Reading last published WAL LSN for: {StreamName}", _options.StreamName);

            var system = await EnsureStreamSystem();

            try
            {
                var stats = await system.StreamStats(_options.StreamName);
                var firstOffset = stats.CommittedChunkId();
            }
            catch (Exception ex)
            {
                _logger?.LogInformation("Empty stream detected: {StreamName} ({ErrorMessage}).", _options.StreamName, ex.Message);
                return 0;
            }

            string? json = null;

            var receivedCount = 0;

            var consumer = await Consumer.Create(new ConsumerConfig(system, _options.StreamName)
            {
                OffsetSpec = new OffsetTypeLast(),
                ClientProvidedName = $"{_options.StreamSystemConfig.ClientProvidedName}-consumer",
                MessageHandler = (stream, consumer, context, message) =>
                {
                    Interlocked.Increment(ref receivedCount);

                    json = Encoding.UTF8.GetString(message.Data.Contents);

                    return Task.CompletedTask;
                }
            }).ConfigureAwait(false);

            // wait until no more messsage are received in the last two seconds
            var lastCount = 0;
            do
            {
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken)
                    .ConfigureAwait(false);

                if (lastCount == receivedCount) break;

                lastCount = receivedCount;
            }
            while (true);

            await consumer.Close()
                .ConfigureAwait(false);

            if (json == null)
            {
                throw new Exception($"Cannot read last WAL end LSN. No messages read from an non-empty stream.");
            }

            if (!json.TryGetWalEnd(out var walEnd))
            {
                throw new Exception($"Missing WAL end LSN in the message: '{json}'");
            }

            _logger?.LogInformation("Last published WAL LSN for {Stream}: {LastWalSeq}", _options.StreamName, walEnd);

            return walEnd;
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