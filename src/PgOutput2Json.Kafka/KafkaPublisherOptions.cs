using Confluent.Kafka;

namespace PgOutput2Json.Kafka
{
    public class KafkaPublisherOptions
    {
        public string Topic { get; set; } = "pgoutput2json";
        public bool WriteHeaders { get; set; } = false;

        public ProducerConfig ProducerConfig { get; set; } = new ProducerConfig
        {
            BootstrapServers = "localhost:9092",
        };
    }
}
