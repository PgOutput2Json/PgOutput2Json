using System;
using System.Collections.Generic;

using RabbitMQ.Client;

namespace PgOutput2Json.RabbitMq
{
    public class RabbitMqPublisherOptions
    {
        public List<string> HostNames { get; set; } = new List<string>();
        public string ExchangeName { get; set; } = "pgoutput2json";

        public ConnectionFactory ConnectionFactory { get; set; } = new ConnectionFactory
        {
            HostName = "localhost",
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            Port = AmqpTcpEndpoint.UseDefaultPort,
            RequestedHeartbeat = TimeSpan.FromSeconds(60),
        };

        /// <summary>
        /// Specify if persistent messages should be used if there is no specific configuration
        /// for the table in <see cref="PersistencyConfigurationByTable"/>.
        /// Default is true.
        /// </summary>
        public bool UsePersistentMessagesByDefault { get; set; } = true;

        /// <summary>
        /// Key is schema qualified table name, for example: public.my_table
        /// Value is true if persistent messages are to be used for that table, false otherwise
        /// </summary>
        public Dictionary<string, bool> PersistencyConfigurationByTable { get; set; }
            = new Dictionary<string, bool>();
    }
}
