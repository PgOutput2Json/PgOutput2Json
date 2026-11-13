using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;
using StackExchange.Redis;

using PgOutput2Json.Redis;

namespace PgOutput2Json.IntegrationTests.Outputs
{
    [TestClass]
    public class RedisPublisherTest
    {
        private static async Task<ConnectionMultiplexer> ConnectAsync()
        {
            return await ConnectionMultiplexer.ConnectAsync(await Containers.GetRedisConnectionStringAsync());
        }

        private static async Task<List<string>> ReadStreamAsync(IDatabase database, string stream)
        {
            return (await database.StreamRangeAsync(stream)).Select(entry => entry["m"].ToString()).ToList();
        }

        [TestMethod]
        public async Task Redis_should_add_the_changes_to_a_stream()
        {
            using var redis = await ConnectAsync();

            var database = redis.GetDatabase();

            await MessageScenario.RunAsync("om_redis",
                builder => builder.UseRedis(options =>
                {
                    options.StreamName = "om_redis";
                    options.Redis = ConfigurationOptions.Parse(redis.Configuration);
                }),
                () => ReadStreamAsync(database, "om_redis"));
        }

        [TestMethod]
        public async Task Redis_should_use_a_stream_per_table_and_partition()
        {
            // a partition count that puts the two rows of the scenario into different partitions
            var partitions = Enumerable.Range(2, 20).First(count => Partition("[1]", count) != Partition("[2]", count));

            using var redis = await ConnectAsync();

            var database = redis.GetDatabase();


            async Task<List<string>> ReadAsync()
            {
                var first = await ReadStreamAsync(database, $"om_redis_p:public.om_redis_p:{Partition("[1]", partitions)}");
                var second = await ReadStreamAsync(database, $"om_redis_p:public.om_redis_p:{Partition("[2]", partitions)}");

                // in the order of the scenario, if each stream has the changes of its row in order
                return first.Count == 2 && second.Count == 2 ? [first[0], second[0], first[1], second[1]] : [.. first, .. second];
            }

            await MessageScenario.RunAsync("om_redis_p",
                builder => builder.UseRedis(options =>
                {
                    options.StreamName = "om_redis_p";
                    options.StreamNameSuffix = StreamNameSuffix.TableNameAndPartition;
                    options.TablePartitionCounts["public.om_redis_p"] = partitions;
                    options.Redis = ConfigurationOptions.Parse(redis.Configuration);
                }),
                ReadAsync);
        }

        [TestMethod]
        public async Task Redis_should_publish_the_changes_to_a_channel_per_table()
        {
            using var redis = await ConnectAsync();

            var received = new ConcurrentQueue<string>();

            // the message queue delivers in order, a plain handler does not
            var queue = await redis.GetSubscriber().SubscribeAsync(RedisChannel.Literal("om_redis_c:public.om_redis_c"));

            queue.OnMessage(message => received.Enqueue(message.Message.ToString()));

            await MessageScenario.RunAsync("om_redis_c",
                builder => builder.UseRedis(options =>
                {
                    options.StreamName = "om_redis_c";
                    options.PublishMode = PublishMode.Channel;
                    options.StreamNameSuffix = StreamNameSuffix.TableName;
                    options.Redis = ConfigurationOptions.Parse(redis.Configuration);
                }),
                () => Task.FromResult(received.ToList()));
        }

        private static int Partition(string key, int count) => (MurmurHash2.Hash(key) & 0x7fffffff) % count;
    }
}
