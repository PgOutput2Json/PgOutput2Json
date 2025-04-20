using RabbitMQ.Stream.Client;

namespace PgOutput2Json.RabbitMqStreams
{
    public class RabbitMqStreamsPublisherOptions
    {
        public string StreamName { get; set; } = "pgoutput2json";

        /// <summary>
        /// Number of the messages sent for each frame-send.<br/>
        /// High values can increase the throughput.<br/>
        /// Low values can reduce the messages latency.<br/>
        /// Default value is 100.
        /// </summary>
        public int MessageBufferSize { get; set; } = 100;


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
