using Microsoft.Extensions.Logging;

namespace PgOutput2Json.RabbitMqStreams
{
    internal class RabbitMqStreamsPublisherFactory : IMessagePublisherFactory
    {
        private readonly RabbitMqStreamsPublisherOptions _options;

        public RabbitMqStreamsPublisherFactory(RabbitMqStreamsPublisherOptions options)
        {
            _options = options;
        }

        public IMessagePublisher CreateMessagePublisher(ReplicationListenerOptions listenerOptions, string slotName, ILoggerFactory? loggerFactory)
        {
            return new RabbitMqStreamsPublisher(_options, listenerOptions.BatchSize, loggerFactory);
        }
    }
}
