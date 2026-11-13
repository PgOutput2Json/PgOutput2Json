using Microsoft.Extensions.Logging;

namespace PgOutput2Json.Kafka
{
    internal class KafkaPublisherFactory : IMessagePublisherFactory
    {
        private readonly KafkaPublisherOptions _options;

        public KafkaPublisherFactory(KafkaPublisherOptions options)
        {
            _options = options;
        }

        public IMessagePublisher CreateMessagePublisher(ReplicationListenerOptions listenerOptions, string slotName, ILoggerFactory? loggerFactory)
        {
            return new KafkaPublisher(_options, loggerFactory?.CreateLogger<KafkaPublisher>());
        }
    }
}
