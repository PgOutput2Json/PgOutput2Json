using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

using StackExchange.Redis;

namespace PgOutput2Json.Redis
{
    public class RedisPublisher : MessagePublisher
    {
        public RedisPublisher(RedisPublisherOptions options, ILogger<RedisPublisher>? logger = null)
        {
            _options = options;
            _logger = logger;
        }

        public override async Task PublishAsync(JsonMessage msg, CancellationToken token)
        {
            _redis ??= await ConnectionMultiplexer.ConnectAsync(_options.Redis)
                .ConfigureAwait(false);

            var json = msg.Json.ToString();
            var tableName = msg.TableName.ToString();
            var partition = GetPartitionId(msg, tableName);

            string name;

            if (_options.StreamNameSuffix == StreamNameSuffix.TableName)
            {
                name = string.Join(':', _options.StreamName, tableName);
            }
            else if (_options.StreamNameSuffix == StreamNameSuffix.TableNameAndPartition)
            {
                name = string.Join(':', _options.StreamName, tableName, partition);
            }
            else
            {
                name = _options.StreamName;
            }

            if (_options.PublishMode == PublishMode.Channel)
            {
                var task = _redis.GetSubscriber().PublishAsync(RedisChannel.Literal(name), json, CommandFlags.DemandMaster);

                _publishedTasks.Add(task);

                if (_logger != null && _logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.LogDebug("Published to Channel={ChannelName}, Body={Body}", name, json);
                }
            }
            else
            {
                var task = _redis.GetDatabase().StreamAddAsync(name, "m", json, flags: CommandFlags.DemandMaster);

                _publishedTasks.Add(task);

                if (_logger != null && _logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.LogDebug("Published to Stream={StreamName}, Body={Body}", name, json);
                }
            }
        }

        private int GetPartitionId(JsonMessage msg, string tableName)
        {
            if (!_options.TablePartitionCounts.TryGetValue(tableName, out var partitionCount) || partitionCount <= 0)
            {
                return 0;
            }

            var partitionKey = msg.PartitionKolValue.ToString();
            if (partitionKey == string.Empty)
            {
                partitionKey = msg.KeyKolValue.ToString();
            }

            // murmur2 - stable across restarts, unlike string.GetHashCode()
            return partitionKey != string.Empty ? (MurmurHash2.Hash(partitionKey) & 0x7fffffff) % partitionCount : 0;
        }

        public override async Task ConfirmAsync(CancellationToken token)
        {
            foreach (var pt in _publishedTasks)
            {
                await pt.ConfigureAwait(false);
            }

            DisposeTasks();
        }

        private void DisposeTasks()
        {
            _publishedTasks.TryDispose(_logger);
            _publishedTasks.Clear();
        }

        public override async ValueTask DisposeAsync()
        {
            DisposeTasks();

            await _redis.TryDisposeAsync(_logger)
                .ConfigureAwait(false);
        }

        private ConnectionMultiplexer? _redis;

        private List<Task> _publishedTasks = [];

        private readonly RedisPublisherOptions _options;
        private readonly ILogger<RedisPublisher>? _logger;
    }
}