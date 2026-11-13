using Confluent.Kafka;
using System;
using System.Collections.Generic;

namespace PgOutput2Json.Kafka
{
    public class KafkaPublisherOptions
    {
        public string Topic { get; set; } = "pgoutput2json";

        /// <summary>
        /// Default is false
        /// </summary>
        public bool WriteHeaders { get; set; } = false;

        /// <summary>
        /// Default is false
        /// </summary>
        public bool WriteTableNameToMessageKey { get; set; } = false;

        /// <summary>
        /// librdkafka producer configuration. Unless set explicitly, the publisher enables idempotence
        /// (<c>EnableIdempotence = true</c>), disables the per-message timeout (<c>MessageTimeoutMs = 0</c>)
        /// and uses the <c>Murmur2Random</c> partitioner. Use <see cref="ConfirmTimeout"/> to limit how long
        /// a batch may take.
        /// </summary>
        public ProducerConfig ProducerConfig { get; set; } = new ProducerConfig
        {
            BootstrapServers = "localhost:9092",
        };

        /// <summary>
        /// Maximum time to wait for Kafka to acknowledge all the messages published since the last confirm.
        /// When it expires, the confirm fails, the producer is discarded, and the listener reconnects and replays
        /// the unconfirmed changes. Zero or <see cref="System.Threading.Timeout.InfiniteTimeSpan"/> waits forever.
        /// Default is 5 minutes.
        /// </summary>
        public TimeSpan ConfirmTimeout { get; set; } = TimeSpan.FromMinutes(5);

        /// <summary>
        /// Used to read the partitions of the topic on start.
        /// If not set, then the client config information is extracted from ProducerConfig.
        /// </summary>
        public AdminClientConfig? AdminClientConfig { get; set; }

        /// <summary>
        /// Override the default partition key fields if needed. The default is the PK fields,
        /// </summary>
        public Dictionary<string, List<string>> PartitionKeyFields { get; set; } = [];
    }
}
