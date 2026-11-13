using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

using Confluent.Kafka;
using System.Text.Json;

namespace PgOutput2Json.Kafka
{
    public class KafkaPublisher: MessagePublisher
    {
        public KafkaPublisher(KafkaPublisherOptions options, ILogger<KafkaPublisher>? logger = null, bool useDeduplication = true)
        {
            _options = options;
            _logger = logger;
            _useDeduplication = useDeduplication;
            _partitionMetadata = GetPartitionMetadata(options);
        }

        public override Task PublishAsync(JsonMessage message, CancellationToken token)
        {
            // fail fast - the listener reconnects without confirming, and resumes from the last published position
            ThrowIfDeliveryFailed();

            var tableName = message.TableName.ToString();
            var msgJson = message.Json.ToString();
            var msgKey = message.KeyKolValue.ToString();

            if (_options.WriteTableNameToMessageKey)
            {
                msgKey = string.Join("", tableName, msgKey);
            }

            var partitionId = GetPartitionId(message, msgKey, out var partitionKey);

            if (_useDeduplication && _dedupSkipActive && IsAlreadyPublished(partitionId, message.TxFinalLsn, message.MessageNo))
            {
                // already processed
                _dedupSkippedCount++;

                return Task.CompletedTask;
            }

            if (_dedupSkipActive && new WalPosition(message.TxFinalLsn, message.MessageNo).IsAfter(_dedupSkipEnd))
            {
                // the replay overlap is over - no further message can be a per-partition
                // duplicate, so summarize the skipped ones and stop the per-message checks
                if (_dedupSkippedCount > 0)
                {
                    _logger?.LogWarning("Deduplication enabled, skipped {SkippedCount} already published messages.", _dedupSkippedCount);
                }

                _dedupSkipActive = false;
            }

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

            producer.Produce(new TopicPartition(_options.Topic, partitionId), new Message<string, string>
            {
                Key = msgKey,
                Value = msgJson,
                Headers = headers
            },
            deliveryReport =>
            {
                // Runs on the librdkafka poll thread, where Confluent.Kafka swallows exceptions - throwing here
                // would let Flush return normally and the WAL be confirmed for an undelivered message.
                // Record the first failure instead, ConfirmAsync and PublishAsync rethrow it.
                if (deliveryReport.Error.IsError)
                {
                    Interlocked.CompareExchange(ref _deliveryError, new ProduceException<string, string>(deliveryReport.Error, deliveryReport), null);
                }
            });

            if (_useDeduplication)
            {
                TrackWalSeq(partitionId, message.TxFinalLsn, message.MessageNo);
            }

            return Task.CompletedTask;
        }

        private int GetPartitionId(JsonMessage message, string msgKey, out string? partitionKey)
        {
            partitionKey = null;

            if (_partitionMetadata.Count == 0) return Partition.Any;

            if (_partitionMetadata.Count < 2)
            {
                // single partition topic
                return _partitionMetadata[0].PartitionId;
            }

            // without deduplication and a partition key there is no predictable target to
            // track - let librdkafka route with the partitioner configured in ProducerConfig
            if (!_useDeduplication && message.PartitionKolValue.Length == 0)
            {
                return Partition.Any;
            }

            string routingKey;

            if (message.PartitionKolValue.Length > 0)
            {
                partitionKey = message.PartitionKolValue.ToString();
                routingKey = partitionKey;
            }
            else
            {
                // no partition key columns are configured - librdkafka would hash the message key,
                // hash it the same way to keep the target partition predictable
                routingKey = msgKey;
            }

            var index = (MurmurHash2.Hash(routingKey) & 0x7fffffff) % _partitionMetadata.Count;

            return _partitionMetadata[index].PartitionId;
        }

        private bool IsAlreadyPublished(int partitionId, ulong txFinalLsn, ulong messageNo)
        {
            return _lastPublished.TryGetValue(partitionId, out var last)
                && new WalPosition(txFinalLsn, messageNo).IsDuplicate(last);
        }

        private void TrackWalSeq(int partitionId, ulong txFinalLsn, ulong messageNo)
        {
            // Partition.Any means partition metadata is missing - the target partition cannot be tracked
            if (partitionId == Partition.Any) return;

            var position = new WalPosition(txFinalLsn, messageNo);

            // messages are published in order, so the position can only move forward
            if (!_lastPublished.TryGetValue(partitionId, out var last) || position.IsAfter(last))
            {
                _lastPublished[partitionId] = position;
            }
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
        /// and the producer is discarded - never single messages, which would leave a gap in a partition.
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

        public override Task<(ulong, ulong)> GetLastPublishedWalSeqAsync(CancellationToken cancellationToken)
        {
            // without deduplication there is no need for the full startup scan
            if (!_useDeduplication) return Task.FromResult((0ul, 0ul));

            if (_logger != null && _logger.IsEnabled(LogLevel.Information))
            {
                _logger.LogInformation("Reading last published WAL LSN for {Topic}", _options.Topic);
            }

            var config = _options.ConsumerConfig ?? new ConsumerConfig(_options.ProducerConfig.ToDictionary());

            config.AutoOffsetReset = AutoOffsetReset.Latest;
            config.GroupId = $"{_options.Topic}-dedupe-{Guid.NewGuid()}";
            config.EnableAutoCommit = false;

            using var consumer = new ConsumerBuilder<string, string>(config).Build();

            _lastPublished.Clear();

            var partitions = new List<TopicPartitionOffset>();

            WalPosition? min = null;

            // Step 1, get partitions offsets
            foreach (var metadata in _partitionMetadata)
            {
                var tpp = new TopicPartition(_options.Topic, new Partition(metadata.PartitionId));

                var endOffsets = consumer.QueryWatermarkOffsets(tpp, TimeSpan.FromSeconds(5));

                // Low == High means there are no readable records - the partition is empty,
                // or its whole history was deleted by retention or compaction
                if (endOffsets.High == 0 || endOffsets.Low >= endOffsets.High)
                {
                    // the partition contributes (0,0) to the minimum, which forces a full
                    // replay - receiving duplicates is safe, a wrong watermark is data loss
                    _lastPublished[metadata.PartitionId] = WalPosition.Zero;

                    min = WalPosition.Zero;

                    if (_logger != null && _logger.IsEnabled(LogLevel.Information))
                    {
                        _logger.LogInformation("No readable messages in topic {Topic}, partition {Partition}", _options.Topic, metadata.PartitionId);
                    }

                    continue;
                }

                // seek to the last message
                partitions.Add(new TopicPartitionOffset(tpp, new Offset(endOffsets.High - 1)));
            }

            // Step 2: Assign manually to specific offsets
            consumer.Assign(partitions);

            var timeouts = 0;
            var pending = new HashSet<int>(partitions.Select(p => p.Partition.Value));

            // Step 3: read the last message of every non-empty partition. Consume returns
            // records from any assigned partition in arrival order, so each record is
            // attributed to the partition it actually came from
            while (pending.Count > 0)
            {
                var record = consumer.Consume(TimeSpan.FromSeconds(5));

                if (record == null)
                {
                    // a partition that holds records can still be slow to fetch - retries are
                    // bounded, after that the listener reconnects and the scan starts over
                    if (++timeouts > 2)
                    {
                        throw new Exception($"Cannot read the last WAL end LSN of topic {_options.Topic} - no messages read from {pending.Count} of {partitions.Count} non-empty partitions.");
                    }

                    continue;
                }

                var partition = record.TopicPartition.Partition.Value;

                if (!pending.Remove(partition)) continue;

                string? value = record.Message.Value;

                WalPosition position;

                if (!value.TryGetWalSeq(out var walSeq, out var messageNo))
                {
                    // a tombstone or a foreign message as the last record - the partition
                    // contributes (0,0) to the minimum, same as an empty partition
                    if (_logger != null && _logger.IsEnabled(LogLevel.Warning))
                    {
                        _logger.LogWarning("Last message in topic {Topic}, partition {Partition} carries no WAL end LSN - the partition contributes (0,0) to the deduplication watermark", _options.Topic, partition);
                    }

                    position = WalPosition.Zero;
                }
                else
                {
                    if (_logger != null && _logger.IsEnabled(LogLevel.Information))
                    {
                        _logger.LogInformation("Last published WAL LSN for topic {Topic}, partition {Partition}: {LastWalSeq}/{LastMessageNo}", record.Topic, partition, walSeq, messageNo);
                    }

                    position = new WalPosition(walSeq, messageNo);
                }

                _lastPublished[partition] = position;

                // the minimum across the partitions is a safe deduplication watermark -
                // everything at or below it is already published to all the partitions
                if (min == null || position.IsAtOrBelow(min.Value))
                {
                    min = position;
                }
            }

            consumer.Close();

            // once the stream passes the highest per-partition watermark, no further message
            // can be a duplicate for any partition - the per-partition checks only run until then
            _dedupSkipEnd = WalPosition.Zero;

            foreach (var position in _lastPublished.Values)
            {
                if (position.IsAfter(_dedupSkipEnd))
                {
                    _dedupSkipEnd = position;
                }
            }

            _dedupSkippedCount = 0;
            _dedupSkipActive = true;

            var watermark = min ?? WalPosition.Zero;

            if (_logger != null && _logger.IsEnabled(LogLevel.Information))
            {
                _logger.LogInformation("Last published WAL LSN for {Topic}: {LastWalSeq}/{LastMessageNo}", _options.Topic, watermark.WalSeq, watermark.MessageNo);
            }

            return Task.FromResult((watermark.WalSeq, watermark.MessageNo));
        }

        private static List<PartitionMetadata> GetPartitionMetadata(KafkaPublisherOptions options)
        {
            var config = options.AdminClientConfig ?? new AdminClientConfig(options.ProducerConfig.ToDictionary());

            using var adminClient = new AdminClientBuilder(config).Build();

            var metadata = adminClient.GetMetadata(options.Topic, TimeSpan.FromSeconds(10));
            var topic = metadata.Topics.FirstOrDefault(t => t.Topic == options.Topic);

            // right after startup (or when the topic does not exist yet) the metadata comes
            // back without partitions - routing on an empty list would fall back to
            // Partition.Any, silently disabling the client-side routing and the per-partition
            // deduplication - so fail immediately and let the listener reconnect
            if (topic == null || topic.Partitions.Count == 0)
            {
                throw new Exception($"Topic {options.Topic} has no partitions - it may not be created yet.");
            }

            return topic.Partitions.ToList();
        }

        private IProducer<string, string> EnsureProducer()
        {
            if (_producer != null) return _producer;

            _logger?.LogInformation("Creating Kafka producer");

            var config = CreateProducerConfig();

            _producer = new ProducerBuilder<string, string>(config)
                .SetErrorHandler((_, e) => _logger?.LogError("Kafka producer error: IsFatal={IsFatal}, Code={Code}, Reason={Reason}", e.IsFatal, e.Code, e.Reason))
                .SetLogHandler((_, e) => _logger?.LogInformation("Kafka producer log: Level={Level}, Message={Message}", e.Level, e.Message))
                .Build();

            _logger?.LogInformation("Created Kafka producer");

            return _producer;
        }

        /// <summary>
        /// Deduplication reads the last message of every partition and treats everything before it as published.
        /// That holds only if each partition receives an unbroken, ordered prefix of the published messages:
        /// - idempotence: the broker rejects out-of-sequence writes, so retries cannot reorder or duplicate
        /// - no per-message timeout: a timed-out message is skipped while later ones are still delivered (a gap)
        /// - gap-less guarantee: any other error that could leave a gap stops the producer with a fatal error
        /// Explicit user settings are kept - librdkafka refuses to start with ones incompatible with idempotence.
        /// The user's config object is copied, not modified.
        /// </summary>
        private ProducerConfig CreateProducerConfig()
        {
            var settings = _options.ProducerConfig.ToDictionary(kv => kv.Key, kv => kv.Value);
            var config = new ProducerConfig(settings);

            config.EnableIdempotence ??= true;

            if (config.EnableIdempotence == true)
            {
                config.EnableGaplessGuarantee ??= true;
            }

            // delivery.timeout.ms is an alias of message.timeout.ms, without a typed property
            settings.TryGetValue("delivery.timeout.ms", out var deliveryTimeout);

            if (config.MessageTimeoutMs == null && deliveryTimeout == null)
            {
                config.MessageTimeoutMs = 0;
            }

            var messageTimeout = config.MessageTimeoutMs?.ToString() ?? deliveryTimeout;

            if (_useDeduplication && (config.EnableIdempotence != true || config.EnableGaplessGuarantee != true || messageTimeout != "0"))
            {
                _logger?.LogWarning("Kafka producer for topic {Topic} runs with EnableIdempotence={EnableIdempotence}, EnableGaplessGuarantee={EnableGaplessGuarantee}, MessageTimeoutMs={MessageTimeoutMs}. " +
                    "A failed delivery can leave a gap in a partition, and deduplication may then skip messages that were never published.",
                    _options.Topic, config.EnableIdempotence, config.EnableGaplessGuarantee, messageTimeout);
            }

            return config;
        }

        private static readonly TimeSpan _flushSlice = TimeSpan.FromMilliseconds(500);
        private static readonly TimeSpan _confirmWarningInterval = TimeSpan.FromSeconds(30);

        private IProducer<string, string>? _producer;

        private ProduceException<string, string>? _deliveryError; // first failed delivery, set on the librdkafka poll thread

        private readonly KafkaPublisherOptions _options;
        private readonly bool _useDeduplication;
        private readonly ILogger<KafkaPublisher>? _logger;

        private List<PartitionMetadata> _partitionMetadata = [];

        private readonly Dictionary<int, WalPosition> _lastPublished = new();

        private bool _dedupSkipActive; // per-partition duplicate checks run only while replaying over the last published positions
        private long _dedupSkippedCount;
        private WalPosition _dedupSkipEnd; // highest per-partition watermark from the startup scan - duplicates are impossible past it
    }
}