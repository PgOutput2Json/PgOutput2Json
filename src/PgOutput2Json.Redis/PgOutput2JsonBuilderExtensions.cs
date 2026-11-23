using PgOutput2Json.Redis;
using System;

namespace PgOutput2Json
{
    public static class PgOutput2JsonBuilderExtensions
    {
        public static PgOutput2JsonBuilder UseRedis(this PgOutput2JsonBuilder builder,
            Action<RedisPublisherOptions>? configureAction = null)
        {
            // it does not know its last position: nothing to skip. WithDeduplication after UseRedis changes it
            builder.WithDeduplication(false);

            var options = new RedisPublisherOptions();

            configureAction?.Invoke(options);

            builder.WithMessagePublisherFactory(new RedisPublisherFactory(options));

            return builder;
        }
    }
}
