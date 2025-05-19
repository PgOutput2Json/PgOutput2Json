using Amazon.Kinesis;

namespace PgOutput2Json.Kinesis
{
    public class KinesisPublisherOptions
    {
        public string StreamName { get; set; } = "pgoutput2json";
        public AmazonKinesisConfig KinesisConfig { get; set; } = new();
    }
}
