using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace PgOutput2Json.Redis
{
    internal class RedisPublisherFactory : IMessagePublisherFactory
    {
        private readonly ConfigurationOptions _options;

        public RedisPublisherFactory(ConfigurationOptions options)
        {
            _options = options;
        }

        public IMessagePublisher CreateMessagePublisher(ILoggerFactory? loggerFactory)
        {
            return new RedisPublisher(_options, loggerFactory?.CreateLogger<RedisPublisher>());
        }
    }
}
