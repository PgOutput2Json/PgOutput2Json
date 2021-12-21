using RabbitMQ.Client;
using System.Collections.Generic;

namespace PgOutput2Json.RabbitMq
{
    public class RabbitMqOptions
    {
        public string[] HostNames { get; set; } = new string[] { "localhost" };
        public string Username { get; set; } = "guest";
        public string Password { get; set; } = "guest";
        public string ExchangeName { get; set; } = "pgoutput2json";
        public string VirtualHost { get; set; } = "/";
        public int Port { get; set; } = AmqpTcpEndpoint.UseDefaultPort;

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
