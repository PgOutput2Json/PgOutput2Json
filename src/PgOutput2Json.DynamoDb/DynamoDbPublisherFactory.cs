using Microsoft.Extensions.Logging;

namespace PgOutput2Json.DynamoDb
{
    internal class DynamoDbPublisherFactory : IMessagePublisherFactory
    {
        private readonly DynamoDbPublisherOptions _options;

        public DynamoDbPublisherFactory(DynamoDbPublisherOptions options)
        {
            _options = options;
        }

        public IMessagePublisher CreateMessagePublisher(ReplicationListenerOptions listenerOptions, string slotName, ILoggerFactory? loggerFactory)
        {
            return new DynamoDbPublisher(_options, loggerFactory?.CreateLogger<DynamoDbPublisher>());
        }
    }
}

