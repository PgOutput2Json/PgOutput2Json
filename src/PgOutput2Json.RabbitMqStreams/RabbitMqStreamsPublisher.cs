using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
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

        /// <summary>
        /// With <see cref="PgOutput2JsonBuilder.WithDeduplication"/>: the position is read from the last message of the stream.
        /// </summary>
        internal bool ReadsLastPosition { get; init; }

        /// <summary>
        /// The position ("w" and "n") of the last message in the stream, which only this listener writes (a single stream,
        /// without the initial copy): the listener skips the replayed changes up to it. (0, 0) for an empty stream.
        /// </summary>
        public override async Task<(ulong, ulong)> GetLastPublishedWalSeqAsync(CancellationToken token)
        {
            if (!ReadsLastPosition) return (0, 0);

            var streamSystem = await EnsureStreamSystemAsync().ConfigureAwait(false);

            ulong lastChunk;

            try
            {
                // the first offset of the last chunk that the stream has committed; it has the last message
                lastChunk = (await streamSystem.StreamStats(_options.StreamName).ConfigureAwait(false)).CommittedChunkId();
            }
            catch (OffsetNotFoundException)
            {
                return (0, 0);
            }

            var last = new TaskCompletionSource<Message>(TaskCreationOptions.RunContinuationsAsynchronously);

            var consumer = await Consumer.Create(new ConsumerConfig(streamSystem, _options.StreamName)
            {
                OffsetSpec = new OffsetTypeOffset(lastChunk),
                ClientProvidedName = $"{_options.StreamSystemConfig.ClientProvidedName}-position",
                MessageHandler = (_, _, context, message) =>
                {
                    if (context.ChunkId == lastChunk && context.Offset == context.ChunkId + context.ChunkMessagesCount - 1)
                    {
                        last.TrySetResult(message);
                    }

                    return Task.CompletedTask;
                },
            }).ConfigureAwait(false);

            try
            {
                using var timeout = CancellationTokenSource.CreateLinkedTokenSource(token);

                timeout.CancelAfter(_options.LastPositionTimeout);

                Message message;

                try
                {
                    message = await last.Task.WaitAsync(timeout.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (!token.IsCancellationRequested)
                {
                    throw new TimeoutException($"The last message of the stream {_options.StreamName} did not arrive in {_options.LastPositionTimeout}");
                }

                var position = ReadPosition(message.Data.Contents)
                    ?? throw new InvalidOperationException($"The last message of the stream {_options.StreamName} has no \"w\" and \"n\": with WithDeduplication(true) only the listener may write to it");

                _logger?.LogInformation("Last position in {StreamName}: {Wal}/{MessageNo}", _options.StreamName, position.Item1, position.Item2);

                return position;
            }
            finally
            {
                await consumer.Close().ConfigureAwait(false);
            }
        }

        /// <summary>The "w" and "n" of a message of the listener, null if it has not both.</summary>
        internal static (ulong, ulong)? ReadPosition(System.Buffers.ReadOnlySequence<byte> json)
        {
            try
            {
                var reader = new Utf8JsonReader(json);

                if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject) return null;

                ulong? w = null, n = null;

                while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
                {
                    var isW = reader.ValueTextEquals("w"u8);
                    var isN = !isW && reader.ValueTextEquals("n"u8);

                    if (!reader.Read()) return null;

                    if (isW && reader.TokenType == JsonTokenType.Number) w = reader.GetUInt64();
                    else if (isN && reader.TokenType == JsonTokenType.Number) n = reader.GetUInt64();
                    else reader.Skip();

                    // both come first, after "c"
                    if (w != null && n != null) return (w.Value, n.Value);
                }

                return null;
            }
            catch (JsonException)
            {
                return null;
            }
        }

        public async override Task PublishAsync(JsonMessage jsonMsg, CancellationToken token)
        {
            // the message keeps its bytes until it is confirmed, so they are its own; made without a string of the JSON
            var msg = new Message(Utf8Buffer.ToArray(jsonMsg.Json))
            {
                Properties = new RabbitMQ.Stream.Client.AMQP.Properties
                {
                    MessageId = _options.WriteTableNameToMessageKey
                        ? string.Join("", jsonMsg.TableName, jsonMsg.KeyKolValue.ToString())
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