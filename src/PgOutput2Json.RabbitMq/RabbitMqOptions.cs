using RabbitMQ.Client;

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
    }
}
