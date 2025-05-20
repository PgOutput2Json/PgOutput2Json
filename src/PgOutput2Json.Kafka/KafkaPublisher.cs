using System;
using System.Collections.Generic;
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
        }

        public override Task PublishAsync(JsonMessage message, CancellationToken token)
        {
            var msgKey = message.KeyKolValue.Length == 0 ? _random.Next().ToString() : message.KeyKolValue.ToString();
            var tableName = message.TableName.ToString();

            if (_options.WriteTableNameToMessageKey) 
            {
                msgKey = string.Join("", tableName, msgKey);
            }

            var producer = EnsureProducer();

            if (_logger != null && _logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug("Publishing to Topic={Topic}, Key={Key}, Body={Body}", _options.Topic, msgKey, message.Json.ToString());
            }

            producer.Produce(_options.Topic, new Message<string, string>
            {
                Key = msgKey,
                Value = message.Json.ToString(),
                Headers = _options.WriteHeaders ? new Headers
                {
                    { "wal_seq_no", Encoding.UTF8.GetBytes(message.WalSeqNo.ToString()) },
                    { "table_name", Encoding.UTF8.GetBytes(tableName) },
                }
                : null,
            });

            return Task.CompletedTask;
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
            _logger?.LogInformation("Reading last published WAL LSN for {Topic}", _options.Topic);

            var config = _options.ConsumerConfig ?? new ConsumerConfig(_options.ProducerConfig.ToDictionary());

            config.AutoOffsetReset = AutoOffsetReset.Latest;
            config.GroupId = $"{_options.Topic}-dedupe-{Guid.NewGuid()}";
            config.EnableAutoCommit = false;

            var partitionMetadata = GetPartitionMetadata();

            using var consumer = new ConsumerBuilder<string, string>(config).Build();

            var partitions = new List<TopicPartitionOffset>();

            // Step 1, get partitions offsets
            foreach (var metadata in partitionMetadata)
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
                var record = consumer.Consume(TimeSpan.FromSeconds(5)) 
                    ?? throw new Exception($"Could not read last WAL LSN from topic {tpo.Topic}, partition {tpo.Partition}");

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

            _logger?.LogInformation("Last published WAL LSN for {Topic}: {LastWalSeq}", _options.Topic, lastWalSeq);

            return Task.FromResult(lastWalSeq);
        }

        private List<PartitionMetadata> GetPartitionMetadata()
        {
            var config = _options.AdminClientConfig ?? new AdminClientConfig(_options.ProducerConfig.ToDictionary());

            using var adminClient = new AdminClientBuilder(config).Build();

            var metadata = adminClient.GetMetadata(_options.Topic, TimeSpan.FromSeconds(10));
            var partitions = metadata.Topics.FirstOrDefault(t => t.Topic == _options.Topic)?.Partitions;

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
        private readonly ILogger<KafkaPublisher>? _logger;

        private readonly Random _random = new();
    }
}