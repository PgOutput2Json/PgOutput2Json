using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

using Confluent.Kafka;

namespace PgOutput2Json.Kafka
{
    public class KafkaPublisher: IMessagePublisher
    {
        public KafkaPublisher(KafkaPublisherOptions options, int batchSize, ILogger<KafkaPublisher>? logger = null)
        {
            _options = options;
            _logger = logger;
        }

        public Task PublishAsync(ulong walSeqNo, string json, string tableName, string keyColumnValue, int partition, CancellationToken token)
        {
            var msgKey = string.IsNullOrEmpty(keyColumnValue) ? tableName : string.Join('|', tableName, keyColumnValue);

            var producer = EnsureProducer();

            _logger?.LogDebug("Publishing to Topic={Topic}, Key={Key}, Body={Body}", _options.Topic, msgKey, json);

            producer.Produce(_options.Topic, new Message<string, string>
            {
                Key = msgKey,
                Value = json,
                Headers = _options.WriteHeaders ? new Headers
                {
                    { "wal_seq_no", Encoding.UTF8.GetBytes(walSeqNo.ToString()) },
                    { "table_name", Encoding.UTF8.GetBytes(tableName) },
                }
                : null,
            });

            return Task.CompletedTask;
        }

        public Task ConfirmAsync(CancellationToken token)
        {
            _producer?.Flush(token);

            return Task.CompletedTask;
        }

        public virtual ValueTask DisposeAsync()
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
    }
}