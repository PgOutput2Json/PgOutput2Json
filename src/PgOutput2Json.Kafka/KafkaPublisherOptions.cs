using Confluent.Kafka;
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

        public ProducerConfig ProducerConfig { get; set; } = new ProducerConfig
        {
            BootstrapServers = "localhost:9092",
        };

        /// <summary>
        /// This is used only if UseDeduplication is set to true, to read the last LSN stored in the topic.
        /// If not set, then the client config information is extracted from ProducerConfig.
        /// </summary>
        public ConsumerConfig? ConsumerConfig { get; set; }

        /// <summary>
        /// This is used only if UseDeduplication is set to true, to read the last LSN stored in the topic.
        /// If not set, then the client config information is extracted from ProducerConfig.
        /// </summary>
        public AdminClientConfig? AdminClientConfig { get; set; }

        /// <summary>
        /// Override the default partition key fields if needed. The default is the PK fields,
        /// </summary>
        public Dictionary<string, List<string>> PartitionKeyFields { get; set; } = [];
    }
}
