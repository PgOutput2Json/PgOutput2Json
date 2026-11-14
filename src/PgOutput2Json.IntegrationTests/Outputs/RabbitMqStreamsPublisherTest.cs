using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;

namespace PgOutput2Json.IntegrationTests.Outputs
{
    [TestClass]
    public class RabbitMqStreamsPublisherTest
    {
        /// <summary>
        /// The broker advertises its own port, not the mapped one, so every connection goes through the resolver.
        /// </summary>
        private static StreamSystemConfig CreateConfig()
        {
            var endpoint = new IPEndPoint(IPAddress.Loopback, Containers.RabbitMqStreamPort);

            return new StreamSystemConfig
            {
                UserName = "rabbitmq",
                Password = "rabbitmq",
                Endpoints = [endpoint],
                AddressResolver = new AddressResolver(endpoint),
            };
        }

        [TestMethod]
        [DataRow("om_stream", false, "[1] [2] [1] [2]")]
        [DataRow("om_stream_t", true, "public.om_stream_t[1] public.om_stream_t[2] public.om_stream_t[1] public.om_stream_t[2]")]
        public async Task RabbitMq_stream_should_get_the_changes_with_the_key_as_the_message_id(string name, bool tableNameInKey, string expectedIds)
        {
            await Containers.GetRabbitMqAsync();

            var system = await StreamSystem.Create(CreateConfig());

            await system.CreateStream(new StreamSpec(name));

            var received = new ConcurrentQueue<(string Body, string? Id)>();

            var consumer = await Consumer.Create(new ConsumerConfig(system, name)
            {
                OffsetSpec = new OffsetTypeFirst(),
                MessageHandler = (_, _, _, message) =>
                {
                    received.Enqueue((Encoding.UTF8.GetString(message.Data.Contents), message.Properties.MessageId?.ToString()));

                    return Task.CompletedTask;
                },
            });

            try
            {
                await MessageScenario.RunAsync(name,
                    builder => builder.UseRabbitMqStreams(options =>
                    {
                        options.StreamName = name;
                        options.WriteTableNameToMessageKey = tableNameInKey;
                        options.StreamSystemConfig = CreateConfig();
                    }),
                    () => Task.FromResult(received.Select(m => m.Body).ToList()));

                Assert.AreEqual(expectedIds, string.Join(" ", received.Select(m => m.Id)));
            }
            finally
            {
                await consumer.Close();
                await system.Close();
            }
        }

        [TestMethod]
        public async Task Super_stream_should_keep_a_row_in_one_partition_in_order()
        {
            var partitions = await PublishToSuperStreamAsync("om_super_key", builder => builder);

            // routed by the message id, which is the key of the row
            AssertRoutedBy(partitions, m => m.Id.ToString());
        }

        [TestMethod]
        public async Task Super_stream_should_route_by_the_partition_key_columns()
        {
            var partitions = await PublishToSuperStreamAsync("om_super_pk", builder => builder.WithPartitionKeyFields("public.om_super_pk", "tenant"));

            AssertRoutedBy(partitions, m => m.Tenant.ToString());

            // seven tenants in three partitions: rows with different keys share a partition only through the tenant
            Assert.IsTrue(partitions.Any(p => p.Select(m => m.Tenant).Distinct().Count() > 1), "no partition has two tenants");
        }

        private const int SuperPartitions = 3;
        private const int SuperRows = 40;

        /// <summary>
        /// Every routing value must be in one partition only, with its changes in the order they were made.
        /// </summary>
        private static void AssertRoutedBy(List<List<(string Change, int Id, int Tenant)>> partitions, System.Func<(string Change, int Id, int Tenant), string> routingValue)
        {
            Assert.IsTrue(partitions.Count(p => p.Count > 0) > 1, "all the messages went to one partition");

            var partitionOf = new Dictionary<string, int>();

            for (var i = 0; i < partitions.Count; i++)
            {
                foreach (var message in partitions[i])
                {
                    var value = routingValue(message);

                    Assert.AreEqual(i, partitionOf.TryGetValue(value, out var known) ? known : partitionOf[value] = i, $"{value} is in two partitions");
                }

                foreach (var row in partitions[i].GroupBy(m => m.Id))
                {
                    Assert.AreEqual("I U", string.Join(" ", row.Select(m => m.Change)), $"row {row.Key}");
                }

                // the inserts are one statement and the updates another, both in the order of the ids
                foreach (var change in partitions[i].GroupBy(m => m.Change))
                {
                    var ids = change.Select(m => m.Id).ToList();

                    CollectionAssert.AreEqual(ids.OrderBy(id => id).ToList(), ids, $"{change.Key} in partition {i}");
                }
            }
        }

        /// <summary>
        /// Inserts and updates rows with a super stream as the output, and returns the messages of each partition.
        /// </summary>
        private static async Task<List<List<(string Change, int Id, int Tenant)>>> PublishToSuperStreamAsync(string name, System.Func<PgOutput2JsonBuilder, PgOutput2JsonBuilder> configure)
        {
            await Containers.GetRabbitMqAsync();

            var system = await StreamSystem.Create(CreateConfig());

            await system.CreateSuperStream(new PartitionsSuperStreamSpec(name, SuperPartitions));

            var received = Enumerable.Range(0, SuperPartitions).Select(_ => new ConcurrentQueue<string>()).ToList();
            var consumers = new List<Consumer>();

            try
            {
                // one consumer per partition stream, to see what each partition has
                for (var i = 0; i < SuperPartitions; i++)
                {
                    var queue = received[i];

                    consumers.Add(await Consumer.Create(new ConsumerConfig(system, $"{name}-{i}")
                    {
                        OffsetSpec = new OffsetTypeFirst(),
                        MessageHandler = (_, _, _, message) =>
                        {
                            queue.Enqueue(Encoding.UTF8.GetString(message.Data.Contents));

                            return Task.CompletedTask;
                        },
                    }));
                }

                await Db.ExecuteAsync($@"
                    CREATE TABLE {name} (id int PRIMARY KEY, tenant int, v text);
                    CREATE PUBLICATION {name}_pub FOR TABLE {name};");

                await Db.CreateSlotAsync($"{name}_slot");

                var builder = configure(PgOutput2JsonBuilder.Create()
                    .WithPgConnectionString(Containers.PgConnectionString)
                    .WithPgPublications($"{name}_pub")
                    .WithPgReplicationSlot($"{name}_slot")
                    .UseRabbitMqStreams(options =>
                    {
                        options.StreamName = name;
                        options.IsSuperStream = true;
                        options.StreamSystemConfig = CreateConfig();
                    }));

                await using (new RunningListener(builder))
                {
                    await Db.ExecuteAsync($"INSERT INTO {name} SELECT i, i % 7, 'a' FROM generate_series(1, {SuperRows}) i");
                    await Db.ExecuteAsync($"UPDATE {name} SET v = 'b'");

                    await Wait.UntilAsync(() => received.Sum(q => q.Count) >= SuperRows * 2, $"{SuperRows * 2} messages in the super stream", 60);
                }

                Assert.AreEqual(SuperRows * 2, received.Sum(q => q.Count));

                return received.Select(queue => queue.Select(body =>
                {
                    using var json = System.Text.Json.JsonDocument.Parse(body);

                    var row = json.RootElement.GetProperty("r");

                    return (json.RootElement.GetProperty("c").GetString()!, row.GetProperty("id").GetInt32(), row.GetProperty("tenant").GetInt32());
                }).ToList()).ToList();
            }
            finally
            {
                foreach (var consumer in consumers) await consumer.Close();

                await system.Close();
            }
        }
    }
}
