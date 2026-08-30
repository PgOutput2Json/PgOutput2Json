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
            if (_logger != null && _logger.IsEnabled(LogLevel.Information))
            {
                _logger.LogInformation("Reading last published WAL LSN for {Topic}", _options.Topic);
            }

            var config = _options.ConsumerConfig ?? new ConsumerConfig(_options.ProducerConfig.ToDictionary());

            config.AutoOffsetReset = AutoOffsetReset.Latest;
            config.GroupId = $"{_options.Topic}-dedupe-{Guid.NewGuid()}";
            config.EnableAutoCommit = false;

            using var consumer = new ConsumerBuilder<string, string>(config).Build();

            var partitions = new List<TopicPartitionOffset>();

            // Step 1, get partitions offsets
            foreach (var metadata in _partitionMetadata)
            {
                var tpp = new TopicPartition(_options.Topic, new Partition(metadata.PartitionId));

                var endOffsets = consumer.QueryWatermarkOffsets(tpp, TimeSpan.FromSeconds(5));

                if (endOffsets.High > 0)
                {
                    // seek to the last message
                    partitions.Add(new TopicPartitionOffset(tpp, new Offset(endOffsets.High - 1)));
                }
                else
                {
                    // empty partition - no message was routed to it, contributes (0,0) to the minimum
                    _lastPublished[metadata.PartitionId] = WalPosition.Zero;
                }
            }

            // Step 2: Assign manually to specific offsets
            consumer.Assign(partitions);

            var min = WalPosition.Zero;
            var hasWatermark = false;

            // Step 3: Poll once per partition
            foreach (var tpo in partitions)
            {
                var record = consumer.Consume(TimeSpan.FromSeconds(5));
                if (record == null)
                {
                    throw new Exception($"Cannot read the last WAL end LSN of topic {tpo.Topic}, partition {tpo.Partition}. No messages read from a non-empty partition.");
                }

                if (!record.Message.Value.TryGetWalSeq(out var walSeq, out var messageNo))
                {
                    throw new Exception($"Missing WAL end LSN in the message: '{record.Message.Value}'");
                }

                if (_logger != null && _logger.IsEnabled(LogLevel.Information))
                {
                    _logger.LogInformation("Last published WAL LSN for topic {Topic}, partition {Partition}: {LastWalSeq}/{LastMessageNo}", tpo.Topic, tpo.Partition, walSeq, messageNo);
                }

                var position = new WalPosition(walSeq, messageNo);

                _lastPublished[tpo.Partition.Value] = position;

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

        private readonly Random _random = new();

        private readonly StringBuilder _partitionKeyBuilder = new();

        private List<PartitionMetadata> _partitionMetadata = [];

        private readonly Dictionary<int, WalPosition> _lastPublished = new();
    }
}