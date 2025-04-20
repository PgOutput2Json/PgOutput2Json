using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

using StackExchange.Redis;

namespace PgOutput2Json.Redis
{
    public class RedisPublisher : IMessagePublisher
    {
        public RedisPublisher(RedisPublisherOptions options, ILogger<RedisPublisher>? logger = null)
        {
            _options = options;
            _logger = logger;
        }

        public async Task PublishAsync(ulong walSeqNo, string json, string tableName, string keyColumnValue, int partition, CancellationToken token)
        {
            _redis ??= await ConnectionMultiplexer.ConnectAsync(_options.Redis)
                .ConfigureAwait(false);

            var channel = RedisChannel.Literal($"{tableName}.{partition}");

            var task = _redis.GetSubscriber().PublishAsync(channel, json);

            _publishedTasks.Add(task);
        }

        public async Task ConfirmAsync(CancellationToken token)
        {
            foreach (var pt in _publishedTasks)
            {
                await pt.ConfigureAwait(false);
            }

            DisposeTasks();
        }

        public Task<ulong> GetLastPublishedWalSeq(CancellationToken token)
        {
            return Task.FromResult(0ul);
        }

        private void DisposeTasks()
        {
            _publishedTasks.TryDispose(_logger);
            _publishedTasks.Clear();
        }

        public virtual async ValueTask DisposeAsync()
        {
            DisposeTasks();

            await _redis.TryDisposeAsync(_logger)
                .ConfigureAwait(false);
        }

        private ConnectionMultiplexer? _redis;
        private List<Task<long>> _publishedTasks = new List<Task<long>>();
        private readonly RedisPublisherOptions _options;
        private readonly ILogger<RedisPublisher>? _logger;
    }
}