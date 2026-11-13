using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

using Confluent.Kafka;

namespace PgOutput2Json.Kafka
{
    public class KafkaPublisher: MessagePublisher
    {
        public KafkaPublisher(KafkaPublisherOptions options, ILogger<KafkaPublisher>? logger = null)
        {
            _options = options;
            _logger = logger;
            _partitionMetadata = GetPartitionMetadata(options);
        }

        public override Task PublishAsync(JsonMessage message, CancellationToken token)
        {
            // fail fast - the listener reconnects without confirming, and PostgreSQL replays the unconfirmed changes
            ThrowIfDeliveryFailed();

            var tableName = message.TableName.ToString();
            var msgJson = message.Json.ToString();
            var msgKey = message.KeyKolValue.ToString();

            if (_options.WriteTableNameToMessageKey)
            {
                msgKey = string.Join("", tableName, msgKey);
            }

            var partition = GetPartition(message, out var partitionKey);

            Headers? headers = null;

            if (_options.WriteHeaders)
            {
                headers = new Headers
                {
                    { "wal_seq_no", Encoding.UTF8.GetBytes(message.TxFinalLsn.ToString()) },
                    { "message_no", Encoding.UTF8.GetBytes(message.MessageNo.ToString()) },
                    { "table_name", Encoding.UTF8.GetBytes(tableName) },
                    { "partition_key", Encoding.UTF8.GetBytes(partitionKey ?? msgKey) }
                };
            }

            var producer = EnsureProducer();

            if (_logger != null && _logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug("Publishing to Topic={Topic}, Key={Key}, Body={Body}", _options.Topic, msgKey, message.Json.ToString());
            }

            producer.Produce(new TopicPartition(_options.Topic, partition), new Message<string, string>
            {
                Key = msgKey,
                Value = msgJson,
                Headers = headers
            },
            OnDelivery);

            return Task.CompletedTask;
        }

        private void OnDelivery(DeliveryReport<string, string> deliveryReport)
        {
            // Runs on the librdkafka poll thread, where Confluent.Kafka swallows exceptions - throwing here
            // would let Flush return normally and the WAL be confirmed for an undelivered message.
            // Record the first failure instead, ConfirmAsync and PublishAsync rethrow it.
            if (deliveryReport.Error.IsError)
            {
                Interlocked.CompareExchange(ref _deliveryError, new ProduceException<string, string>(deliveryReport.Error, deliveryReport), null);
            }
        }

        /// <summary>
        /// Without partition key columns librdkafka routes by the message key, with the partitioner configured
        /// in ProducerConfig (murmur2_random by default). A partition key differs from the message key, so it is
        /// hashed here, the same way: (murmur2 &amp; 0x7fffffff) % partition count.
        /// </summary>
        private Partition GetPartition(JsonMessage message, out string? partitionKey)
        {
            partitionKey = null;

            if (message.PartitionKolValue.Length == 0) return Partition.Any;

            partitionKey = message.PartitionKolValue.ToString();

            var index = (MurmurHash2.Hash(partitionKey) & 0x7fffffff) % _partitionMetadata.Count;

            return _partitionMetadata[index].PartitionId;
        }

        public override Task ConfirmAsync(CancellationToken token)
        {
            if (_producer != null)
            {
                WaitForDelivery(_producer, token);
            }

            // Flush returns normally even if some deliveries failed - they are reported only to the handler
            ThrowIfDeliveryFailed();

            return Task.CompletedTask;
        }

        /// <summary>
        /// Waits until Kafka acknowledged every message published since the last confirm. The per-message timeout
        /// is disabled by default, so the deadline applies to the batch as a whole: when it expires the confirm fails
        /// and the producer is discarded - never single messages, which would leave a gap in the order.
        /// </summary>
        private void WaitForDelivery(IProducer<string, string> producer, CancellationToken token)
        {
            var timeout = _options.ConfirmTimeout;
            var waitForever = timeout <= TimeSpan.Zero || timeout == Timeout.InfiniteTimeSpan;

            var stopwatch = Stopwatch.StartNew();
            var nextWarning = _confirmWarningInterval;

            while (true)
            {
                token.ThrowIfCancellationRequested();

                // short slices keep the wait cancellable and let the progress be logged
                var outstanding = producer.Flush(_flushSlice);

                if (outstanding == 0) return;

                // a failed delivery fails the confirm anyway, no point waiting for the rest of the batch
                ThrowIfDeliveryFailed();

                var elapsed = stopwatch.Elapsed;

                if (!waitForever && elapsed >= timeout)
                {
                    throw new TimeoutException($"Kafka did not acknowledge {outstanding} messages for topic {_options.Topic} within {timeout}");
                }

                if (elapsed >= nextWarning)
                {
                    _logger?.LogWarning("Waiting for Kafka to acknowledge {Outstanding} messages for topic {Topic} for {Elapsed}",
                        outstanding, _options.Topic, elapsed);

                    nextWarning += _confirmWarningInterval;
                }
            }
        }

        private void ThrowIfDeliveryFailed()
        {
            var error = Volatile.Read(ref _deliveryError);

            if (error != null)
            {
                throw new Exception($"Kafka delivery to topic {_options.Topic} failed: {error.Error.Reason}", error);
            }
        }

        public override ValueTask DisposeAsync()
        {
            if (_producer != null)
            {
                try
                {
                    _producer.Dispose();
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Error closing Kafka connection");
                }
            }

            return ValueTask.CompletedTask;
        }

        private static List<PartitionMetadata> GetPartitionMetadata(KafkaPublisherOptions options)
        {
            var config = options.AdminClientConfig ?? new AdminClientConfig(options.ProducerConfig.ToDictionary());

            using var adminClient = new AdminClientBuilder(config).Build();

            var metadata = adminClient.GetMetadata(options.Topic, TimeSpan.FromSeconds(10));
            var topic = metadata.Topics.FirstOrDefault(t => t.Topic == options.Topic);

            // right after startup (or when the topic does not exist yet) the metadata comes
            // back without partitions - a partition key cannot be routed on an empty list,
            // so fail immediately and let the listener reconnect
            if (topic == null || topic.Partitions.Count == 0)
            {
                throw new Exception($"Topic {options.Topic} has no partitions - it may not be created yet.");
            }

            // ordered by id, so a partition key maps to the same partition on every start
            return topic.Partitions.OrderBy(p => p.PartitionId).ToList();
        }

        private IProducer<string, string> EnsureProducer()
        {
            if (_producer != null) return _producer;

            _logger?.LogInformation("Creating Kafka producer");

            var config = CreateProducerConfig(_options);

            _producer = new ProducerBuilder<string, string>(config)
                .SetErrorHandler((_, e) => _logger?.LogError("Kafka producer error: IsFatal={IsFatal}, Code={Code}, Reason={Reason}", e.IsFatal, e.Code, e.Reason))
                .SetLogHandler((_, e) => _logger?.LogInformation("Kafka producer log: Level={Level}, Message={Message}", e.Level, e.Message))
                .Build();

            _logger?.LogInformation("Created Kafka producer");

            return _producer;
        }

        /// <summary>
        /// Defaults that keep the messages of a partition in WAL order:
        /// - idempotence: the broker rejects out-of-sequence writes, so retries cannot reorder or duplicate
        /// - no per-message timeout: a timed-out message is skipped while later ones are still delivered (a gap)
        /// - murmur2_random partitioner: the Kafka default, and the hash used here for partition key columns
        /// A message the broker rejects can still leave a gap, which the replay then fills out of order -
        /// EnableGaplessGuarantee prevents that, but it is experimental in librdkafka, so it is left to the user.
        /// Explicit user settings are kept - librdkafka refuses to start with ones incompatible with idempotence.
        /// The user's config object is copied, not modified.
        /// </summary>
        internal static ProducerConfig CreateProducerConfig(KafkaPublisherOptions options)
        {
            var settings = options.ProducerConfig.ToDictionary(kv => kv.Key, kv => kv.Value);
            var config = new ProducerConfig(settings);

            config.EnableIdempotence ??= true;

            // delivery.timeout.ms is an alias of message.timeout.ms, without a typed property
            if (config.MessageTimeoutMs == null && !settings.ContainsKey("delivery.timeout.ms"))
            {
                config.MessageTimeoutMs = 0;
            }

            config.Partitioner ??= Partitioner.Murmur2Random;

            return config;
        }

        private static readonly TimeSpan _flushSlice = TimeSpan.FromMilliseconds(500);
        private static readonly TimeSpan _confirmWarningInterval = TimeSpan.FromSeconds(30);

        private IProducer<string, string>? _producer;

        private ProduceException<string, string>? _deliveryError; // first failed delivery, set on the librdkafka poll thread

        private readonly KafkaPublisherOptions _options;
        private readonly ILogger<KafkaPublisher>? _logger;

        private readonly List<PartitionMetadata> _partitionMetadata;
    }
}