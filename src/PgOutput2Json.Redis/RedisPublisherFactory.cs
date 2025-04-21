using Microsoft.Extensions.Logging;

namespace PgOutput2Json.Redis
{
    internal class RedisPublisherFactory : IMessagePublisherFactory
    {
        private readonly RedisPublisherOptions _options;

        public RedisPublisherFactory(RedisPublisherOptions options)
        {
            _options = options;
        }

        public IMessagePublisher CreateMessagePublisher(ReplicationListenerOptions listenerOptions, ILoggerFactory? loggerFactory)
        {
            return new RedisPublisher(_options, loggerFactory?.CreateLogger<RedisPublisher>());
        }
    }
}
