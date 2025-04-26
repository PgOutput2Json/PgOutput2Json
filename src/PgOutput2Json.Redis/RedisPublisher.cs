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

        public override async Task PublishAsync(ulong walSeqNo, string json, string tableName, string keyColumnValue, int partition, CancellationToken token)
        {
            _redis ??= await ConnectionMultiplexer.ConnectAsync(_options.Redis)
                .ConfigureAwait(false);

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

        public override async Task ConfirmAsync(CancellationToken token)
        {
            foreach (var pt in _publishedTasks)
            {
                await pt.ConfigureAwait(false);
            }

            DisposeTasks();
        }

        public override async Task<ulong> GetLastPublishedWalSeq(CancellationToken token)
        {
            if (_options.PublishMode == PublishMode.Channel) return 0; // cannot do de-duplication with channels

            _redis ??= await ConnectionMultiplexer.ConnectAsync(_options.Redis)
                .ConfigureAwait(false);

            var entries = _redis.GetDatabase().StreamRange(_options.StreamName, "-", "+", count: 1, messageOrder: Order.Descending, flags: CommandFlags.DemandMaster);

            if (entries.Length == 0)
            {
                return 0;
            }

            var lastEntry = entries[^1];

            if (lastEntry.Values.Length == 0)
            {
                throw new Exception($"Could not rad WAL end LSN - missing entry value");
            }

            var json = lastEntry.Values[0].Value.ToString();

            if (string.IsNullOrEmpty(json))
            {
                throw new Exception($"Could not rad WAL end LSN - entry value is null or empty");
            }

            if (!json.TryGetWalEnd(out var walEnd))
            {
                throw new Exception($"Missing WAL end LSN in the message: '{json}'");
            }

            return walEnd;
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