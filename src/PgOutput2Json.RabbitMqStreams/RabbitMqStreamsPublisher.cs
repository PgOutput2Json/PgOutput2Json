using System;
using System.Collections.Generic;
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
            // the message keeps its bytes until it is confirmed, so they are its own; made without a string of the JSON
            var msg = new Message(GetUtf8(jsonMsg.Json))
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

            var producer = await EnsureProducerAsync().ConfigureAwait(false);

            lock (_confirmationLock)
            {
                // A failed confirmation must fail the batch. Replacing the faulted source would lose the failure,
                // and since the failed message is never counted as confirmed, the confirm would hang forever.
                if (_confirmationTaskCompletionSource?.Task.Exception is { } failure)
                {
                    throw failure.InnerException ?? failure;
                }

                _unconfirmedCount++;

                if (_confirmationTaskCompletionSource == null || _confirmationTaskCompletionSource.Task.IsCompleted)
                {
                    _confirmationTaskCompletionSource = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                }
            }

            if (_options.IsSuperStream)
            {
                // a super stream routes each message to its partition
                await producer.Send(msg).ConfigureAwait(false);
            }
            else
            {
                // sent in lists of MessageBufferSize: the client sends what it has as soon as it can, so messages that come
                // a little slower than it sends would go a few in a frame, with a write and a wake-up of a thread for each
                _pending.Add(msg);

                if (_pending.Count >= _options.MessageBufferSize) await SendPendingAsync(producer).ConfigureAwait(false);
            }

            if (_logger != null && _logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug("Published to {StreamName}: {Body}", _options.StreamName, jsonMsg.Json.ToString());
            }
        }

        private readonly Encoder _encoder = Encoding.UTF8.GetEncoder();

        private byte[] GetUtf8(StringBuilder text)
        {
            // an encoder, because a chunk of the StringBuilder can end in the middle of a surrogate pair
            _encoder.Reset();

            var length = 0;

            foreach (var chunk in text.GetChunks()) length += _encoder.GetByteCount(chunk.Span, flush: false);

            length += _encoder.GetByteCount(System.ReadOnlySpan<char>.Empty, flush: true);

            var bytes = new byte[length];

            _encoder.Reset();

            var written = 0;

            foreach (var chunk in text.GetChunks()) written += _encoder.GetBytes(chunk.Span, bytes.AsSpan(written), flush: false);

            _encoder.GetBytes(System.ReadOnlySpan<char>.Empty, bytes.AsSpan(written), flush: true);

            return bytes;
        }

        private async Task SendPendingAsync(Producer producer)
        {
            // the client keeps the list, so the next messages go in a new one
            var messages = _pending;

            _pending = new List<Message>(_options.MessageBufferSize);

            await producer.Send(messages).ConfigureAwait(false);
        }

        public async override Task ConfirmAsync(CancellationToken token)
        {
            _checkProducer = true;

            if (_pending.Count > 0 && _producer != null) await SendPendingAsync(_producer).ConfigureAwait(false);

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
            // IsOpen is not cheap, so it is asked once a batch: a producer that closes in a batch fails its sends or
            // its confirmations, which fails the batch, and the listener connects again with a new publisher
            if (_producer != null && (!_checkProducer || _producer.IsOpen()))
            {
                _checkProducer = false;
                return _producer;
            }

            _checkProducer = false;

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
                                        _confirmationTaskCompletionSource?.TrySetResult();
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
                                    _confirmationTaskCompletionSource?.TrySetException(new Exception(
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

        private StreamSystem? _streamSystem;
        private Producer? _producer;
        private bool _checkProducer = true;

        private List<Message> _pending = [];

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