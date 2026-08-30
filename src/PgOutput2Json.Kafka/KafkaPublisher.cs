using System;
using System.Collections.Generic;
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
            var tableName = message.TableName.ToString();
            var msgJson = message.Json.ToString();
            var msgKey = message.KeyKolValue.ToString();

            if (_options.WriteTableNameToMessageKey)
            {
                msgKey = string.Join("", tableName, msgKey);
            }

            var partitionId = GetPartitionId(message, msgKey, out var partitionKey);

            if (_useDeduplication && IsAlreadyPublished(partitionId, message.TxFinalLsn, message.MessageNo))
            {
                _logger?.LogWarning("Skipping already published message for topic {Topic}, partition {Partition}: " +
                    "TX Final LSN = {TxFinalLsn}, MessageNo = {MessageNo}",
                    _options.Topic, partitionId, message.TxFinalLsn, message.MessageNo);

                return Task.CompletedTask;
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
                if (deliveryReport.Error.IsError)
                {
                    throw new Exception(deliveryReport.Error.Reason);
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
            _producer?.Flush(token);

            return Task.CompletedTask;
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

            var min = WalPosition.Zero;
            var hasWatermark = false;
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
                if (!hasWatermark || position.IsAtOrBelow(min))
                {
                    hasWatermark = true;
                    min = position;
                }
            }

            consumer.Close();

            if (_logger != null && _logger.IsEnabled(LogLevel.Information))
            {
                _logger.LogInformation("Last published WAL LSN for {Topic}: {LastWalSeq}/{LastMessageNo}", _options.Topic, min.WalSeq, min.MessageNo);
            }

            return Task.FromResult((min.WalSeq, min.MessageNo));
        }

        private static List<PartitionMetadata> GetPartitionMetadata(KafkaPublisherOptions options)
        {
            var config = options.AdminClientConfig ?? new AdminClientConfig(options.ProducerConfig.ToDictionary());

            using var adminClient = new AdminClientBuilder(config).Build();

            var metadata = adminClient.GetMetadata(options.Topic, TimeSpan.FromSeconds(10));
            var partitions = metadata.Topics.FirstOrDefault(t => t.Topic == options.Topic)?.Partitions;

            return partitions ?? [];
        }

        private IProducer<string, string> EnsureProducer()
        {
            if (_producer != null) return _producer;

            _logger?.LogInformation("Creating Kafka producer");

            _producer = new ProducerBuilder<string, string>(_options.ProducerConfig)
                .SetErrorHandler((_, e) => _logger?.LogError("Kafka producer error: IsFatal={IsFatal}, Code={Code}, Reason={Reason}", e.IsFatal, e.Code, e.Reason))
                .SetLogHandler((_, e) => _logger?.LogInformation("Kafka producer log: Level={Level}, Message={Message}", e.Level, e.Message))
                .Build();

            _logger?.LogInformation("Created Kafka producer");

            return _producer;
        }

        private IProducer<string, string>? _producer;

        private readonly KafkaPublisherOptions _options;
        private readonly bool _useDeduplication;
        private readonly ILogger<KafkaPublisher>? _logger;

        private List<PartitionMetadata> _partitionMetadata = [];

        private readonly Dictionary<int, WalPosition> _lastPublished = new();
    }
}