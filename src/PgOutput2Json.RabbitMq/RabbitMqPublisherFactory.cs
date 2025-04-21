using Microsoft.Extensions.Logging;

namespace PgOutput2Json.RabbitMq
{
    internal class RabbitMqPublisherFactory : IMessagePublisherFactory
    {
        private readonly RabbitMqPublisherOptions _options;

        public RabbitMqPublisherFactory(RabbitMqPublisherOptions options)
        {
            _options = options;
        }

        public IMessagePublisher CreateMessagePublisher(ReplicationListenerOptions listenerOptions, ILoggerFactory? loggerFactory)
        {
            return new RabbitMqPublisher(_options, listenerOptions.BatchSize, loggerFactory?.CreateLogger<RabbitMqPublisher>());
        }
    }
}
