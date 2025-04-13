using RabbitMQ.Stream.Client;

namespace PgOutput2Json.RabbitMqStreams
{
    public class RabbitMqStreamsPublisherOptions
    {
        public string StreamName { get; set; } = "pgoutput2json";
        public bool UseDeduplication { get; set; } = false;

        public StreamSystemConfig StreamSystemConfig { get; set; } = new StreamSystemConfig
        {
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            Endpoints = [],

            ClientProvidedName = "PgOutput2Json",
        };
    }
}
