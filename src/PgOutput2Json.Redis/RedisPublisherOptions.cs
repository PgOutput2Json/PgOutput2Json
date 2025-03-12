using StackExchange.Redis;

namespace PgOutput2Json.Redis
{
    public class RedisPublisherOptions
    {
        public ConfigurationOptions Redis { get; set; } = new ConfigurationOptions();
    }
}
