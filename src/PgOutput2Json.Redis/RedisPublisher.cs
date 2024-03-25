using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

using StackExchange.Redis;

namespace PgOutput2Json.Redis
{
    public class RedisPublisher : IMessagePublisher
    {
        public RedisPublisher(ConfigurationOptions options, ILogger<RedisPublisher>? logger = null)
        {
            _options = options;
            _logger = logger;
        }

        public void Dispose()
        {
            CloseConnection();
        }

        public void Publish(string json, string tableName, string keyColumnValue, int partition)
        {
            EnsureConnection();

            try
            {
                var channel = RedisChannel.Literal($"{tableName}.{partition}");

                var task = _redis!.GetSubscriber().PublishAsync(channel, json);

                var prefix = _options.ChannelPrefix;

                _publishedTasks.Add(task);
            }
            catch 
            {
                CloseConnection();
                throw;
            }
        }

        public void WaitForConfirmsOrDie(TimeSpan timeout)
        {
            if (_publishedTasks.Count == 0) return;

            try
            {
                Task.WaitAll(_publishedTasks.ToArray());

                SafeLogInfo($"{_publishedTasks.Count} async Redis operations completed");

                DisposeTasks();
            }
            catch
            {
                CloseConnection();
                throw;
            }
        }

        private void EnsureConnection()
        {
            if (_redis != null) return;

            try
            {
                _redis = ConnectionMultiplexer.Connect(_options);
            }
            catch
            {
                CloseConnection();
                throw;
            }
        }

        private void CloseConnection()
        {
            DisposeTasks();

            _redis.TryDispose(_logger);
            _redis = null;
        }

        private void DisposeTasks()
        {
            _publishedTasks.TryDispose(_logger);
            _publishedTasks.Clear();
        }

        private void SafeLogInfo(string message)
        {
            try
            {
                _logger?.LogInformation(message);
            }
            catch
            {
            }
        }

        private ConnectionMultiplexer? _redis;
        private List<Task<long>> _publishedTasks = new List<Task<long>>();
        private readonly ConfigurationOptions _options;
        private readonly ILogger<RedisPublisher>? _logger;
    }
}