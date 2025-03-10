﻿using StackExchange.Redis;

namespace PgOutput2Json.Redis
{
    public class RedisPublisherOptions
    {
        public ConfigurationOptions Redis { get; set; } = new ConfigurationOptions();
        public int BatchSize { get; set; } = 100;
    }
}
