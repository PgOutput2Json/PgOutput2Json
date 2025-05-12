using Microsoft.Extensions.Logging;

namespace PgOutput2Json.MongoDb
{
    internal class MongoDbPublisherFactory : IMessagePublisherFactory
    {
        private readonly MongoDbPublisherOptions _options;

        public MongoDbPublisherFactory(MongoDbPublisherOptions options)
        {
            _options = options;
        }

        public IMessagePublisher CreateMessagePublisher(ReplicationListenerOptions listenerOptions, ILoggerFactory? loggerFactory)
        {
            return new MongoDbPublisher(_options, loggerFactory?.CreateLogger<MongoDbPublisher>());
        }
    }
}
