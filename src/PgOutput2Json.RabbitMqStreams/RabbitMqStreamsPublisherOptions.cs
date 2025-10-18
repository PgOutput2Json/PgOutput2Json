using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using System;

namespace PgOutput2Json.RabbitMqStreams
{
    public class RabbitMqStreamsPublisherOptions
    {
        public string StreamName { get; set; } = "pgoutput2json";

        /// <summary>
        /// Default is false
        /// </summary>
        public bool WriteTableNameToMessageKey { get; set; }
        
        public bool IsSuperStream { get; set; }

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

        public SuperStreamConfig SuperStreamConfig { get; set; } = new SuperStreamConfig
        {
            Routing = msg =>
            {
                return msg.ApplicationProperties != null
                    && msg.ApplicationProperties.TryGetValue(ApplicationProperties.PartitionKey, out var partitionKey)
                    && partitionKey != null
                    ? partitionKey.ToString()
                    : msg.Properties?.MessageId?.ToString() ?? Guid.NewGuid().ToString();
            },
            RoutingStrategyType = RoutingStrategyType.Hash,
        };
    }
}
