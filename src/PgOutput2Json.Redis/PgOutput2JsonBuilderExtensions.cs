using System;

namespace PgOutput2Json.Redis
{
    public static class PgOutput2JsonBuilderExtensions
    {
        public static PgOutput2JsonBuilder UseRedis(this PgOutput2JsonBuilder builder,
            Action<RedisPublisherOptions>? configureAction = null)
        {
            var options = new RedisPublisherOptions();

            configureAction?.Invoke(options);

            builder.WithMessagePublisherFactory(new RedisPublisherFactory(options));

            return builder;
        }
    }
}
