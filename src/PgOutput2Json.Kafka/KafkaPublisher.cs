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
        public KafkaPublisher(KafkaPublisherOptions options, ILogger<KafkaPublisher>? logger = null)
        {
            _options = options;
            _logger = logger;
            _partitionMetadata = GetPartitionMetadata(options);
        }

        public override Task PublishAsync(JsonMessage message, CancellationToken token)
        {
            var tableName = message.TableName.ToString();
            var msgJson = message.Json.ToString();
            var msgKey = message.KeyKolValue.ToString();

            var partitionKey = GetPartitionKey(msgJson, tableName);
            var partitionId = GetPartitionId(partitionKey);

            if (_options.WriteTableNameToMessageKey) 
            {
                msgKey = string.Join("", tableName, msgKey);
            }

            Headers? headers = null;

            if (_options.WriteHeaders)
            {
                headers = new Headers
                {
                    { "wal_seq_no", Encoding.UTF8.GetBytes(message.WalSeqNo.ToString()) },
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
            
            return Task.CompletedTask;
        }

        private int GetPartitionId(string? partitionKey)
        {
            if (partitionKey == null || _partitionMetadata.Count == 0) return Partition.Any;

            var index = Math.Abs(partitionKey.GetHashCode()) % _partitionMetadata.Count;
            return _partitionMetadata[index].PartitionId;
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

        public override Task<ulong> GetLastPublishedWalSeqAsync(CancellationToken cancellationToken)
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
            }

            // Step 2: Assign manually to specific offsets
            consumer.Assign(partitions);

            var lastWalSeq = 0ul;

            // Step 3: Poll once per partition
            foreach (var tpo in partitions)
            {
                var record = consumer.Consume(TimeSpan.FromSeconds(5));
                if (record == null)
                {
                    if (_logger != null && _logger.IsEnabled(LogLevel.Warning))
                    {
                        _logger.LogWarning("Empty record returned when reading last WAL LSN from topic {Topic}, partition {Partition}", tpo.Topic, tpo.Partition);
                    }
                    continue; 
                }
              
                if (!record.Message.Value.TryGetWalEnd(out var walSeq))
                {
                    throw new Exception($"Missing WAL end LSN in the message: '{record.Message.Value}'");
                }

                if (walSeq > lastWalSeq)
                {
                    lastWalSeq = walSeq;
                }
            }

            consumer.Close();

            if (_logger != null && _logger.IsEnabled(LogLevel.Information))
            {
                _logger.LogInformation("Last published WAL LSN for {Topic}: {LastWalSeq}", _options.Topic, lastWalSeq);
            }

            return Task.FromResult(lastWalSeq);
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

        private string? GetPartitionKey(string msgJson, string tableName)
        {
            if (!_options.PartitionKeyFields.TryGetValue(tableName, out var fields))
            {
                return null;
            }

            using var doc = JsonDocument.Parse(msgJson);

            // use the new values if available
            if (!doc.RootElement.TryGetProperty("r", out var rowElement))
            {
                // for deletes use the key values
                if (!doc.RootElement.TryGetProperty("k", out rowElement))
                {
                    // should never happen (we alwayes have either r or k or both)
                    return null;
                }
            }

            _partitionKeyBuilder.Clear();
            _partitionKeyBuilder.Append('[');

            foreach (var field in fields)
            {
                if (rowElement.TryGetProperty(field, out var fieldElement))
                {
                    if (_partitionKeyBuilder.Length > 1)
                    {
                        _partitionKeyBuilder.Append(',');
                    }

                    _partitionKeyBuilder.Append(fieldElement.GetRawText());
                }
            }

            _partitionKeyBuilder.Append(']');

            return _partitionKeyBuilder.Length > 2 ? _partitionKeyBuilder.ToString() : null;
        }

        private IProducer<string, string>? _producer;

        private readonly KafkaPublisherOptions _options;
        private readonly ILogger<KafkaPublisher>? _logger;

        private readonly Random _random = new();

        private readonly StringBuilder _partitionKeyBuilder = new();

        private List<PartitionMetadata> _partitionMetadata = [];
    }
}