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

        public IMessagePublisher CreateMessagePublisher(int batchSize, ILoggerFactory? loggerFactory)
        {
            return new RabbitMqPublisher(_options, batchSize, loggerFactory?.CreateLogger<RabbitMqPublisher>());
        }
    }
}
