using Microsoft.Extensions.Logging;
using PgOutput2Json.Core;

namespace PgOutput2Json.RabbitMq
{
    internal class RabbitMqPublisherFactory : IMessagePublisherFactory
    {
        private readonly RabbitMqOptions _options;

        public RabbitMqPublisherFactory(RabbitMqOptions options)
        {
            _options = options;
        }

        public IMessagePublisher CreateMessagePublisher(ILoggerFactory? loggerFactory)
        {
            return new RabbitMqPublisher(_options, loggerFactory?.CreateLogger<RabbitMqPublisher>());
        }
    }
}
