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

        /// <summary>
        /// The ids of the rows in the stream, in its order.
        /// </summary>
        private static async Task<List<int>> ReadIdsAsync(StreamSystem system, string stream, int count)
        {
            var ids = new ConcurrentQueue<int>();

            var consumer = await Consumer.Create(new ConsumerConfig(system, stream)
            {
                OffsetSpec = new OffsetTypeFirst(),
                MessageHandler = (_, _, _, message) =>
                {
                    using var json = System.Text.Json.JsonDocument.Parse(message.Data.Contents);

                    ids.Enqueue(json.RootElement.GetProperty("r").GetProperty("id").GetInt32());

                    return Task.CompletedTask;
                },
            });

            try
            {
                await Wait.UntilAsync(() => ids.Count >= count, $"{count} messages in the stream", 60);
            }
            finally
            {
                await consumer.Close();
            }

            return ids.ToList();
        }

        /// <summary>
        /// A listener writes rows 1 to 501 to an empty stream, then one on a slot that stayed at the start gets them
        /// again, and row 502. With deduplication it skips what the stream has (its last message), else publishes it again.
        /// </summary>
        [TestMethod]
        [DataRow("om_dedup", true, 502)]
        [DataRow("om_dedup_off", false, 1003)]
        public async Task Deduplication_should_skip_what_the_stream_has_after_a_restart(string name, bool deduplication, int expectedCount)
        {
            await Containers.GetRabbitMqAsync();

            var system = await StreamSystem.Create(CreateConfig());

            await system.CreateStream(new StreamSpec(name));

            await Db.ExecuteAsync($@"
                CREATE TABLE {name} (id int PRIMARY KEY, v text);
                CREATE PUBLICATION {name}_pub FOR TABLE {name};");

            await Db.CreateSlotAsync($"{name}_slot");
            await Db.CreateSlotAsync($"{name}_replay_slot");

            PgOutput2JsonBuilder CreateBuilder(string slotName) => PgOutput2JsonBuilder.Create()
                .WithPgConnectionString(Containers.PgConnectionString)
                .WithPgPublications($"{name}_pub")
                .WithPgReplicationSlot(slotName)
                .UseRabbitMqStreams(options =>
                {
                    options.StreamName = name;
                    options.StreamSystemConfig = CreateConfig();
                })
                .WithDeduplication(deduplication);

            try
            {
                await using (new RunningListener(CreateBuilder($"{name}_slot")))
                {
                    await Db.ExecuteAsync($"INSERT INTO {name} SELECT i, 'a' FROM generate_series(1, 500) i");
                    await Db.ExecuteAsync($"INSERT INTO {name} VALUES (501, 'b')");

                    await ReadIdsAsync(system, name, 501);
                }

                await Db.ExecuteAsync($"INSERT INTO {name} VALUES (502, 'c')");

                await using (new RunningListener(CreateBuilder($"{name}_replay_slot")))
                {
                    // 502 is last in the WAL, after any replayed row
                    await ReadIdsAsync(system, name, expectedCount);
                }

                var expected = deduplication
                    ? Enumerable.Range(1, 502)
                    : Enumerable.Range(1, 501).Concat(Enumerable.Range(1, 502));

                CollectionAssert.AreEqual(expected.ToList(), await ReadIdsAsync(system, name, expectedCount));
            }
            finally
            {
                await system.Close();
            }
        }

        /// <summary>
        /// Retention removes whole segments of the stream, never the one it writes to: the last message stays,
        /// so a restart finds it although the first messages are gone.
        /// </summary>
        [TestMethod]
        public async Task Deduplication_should_find_the_last_message_after_retention()
        {
            const string name = "om_dedup_ret";

            await Containers.GetRabbitMqAsync();

            var system = await StreamSystem.Create(CreateConfig());

            // segments of about 20 messages, and only two of them kept
            await system.CreateStream(new StreamSpec(name) { MaxSegmentSizeBytes = 5_000, MaxLengthBytes = 10_000 });

            await Db.ExecuteAsync($@"
                CREATE TABLE {name} (id int PRIMARY KEY, v text);
                CREATE PUBLICATION {name}_pub FOR TABLE {name};");

            await Db.CreateSlotAsync($"{name}_slot");
            await Db.CreateSlotAsync($"{name}_replay_slot");

            PgOutput2JsonBuilder CreateBuilder(string slotName) => PgOutput2JsonBuilder.Create()
                .WithPgConnectionString(Containers.PgConnectionString)
                .WithPgPublications($"{name}_pub")
                .WithPgReplicationSlot(slotName)
                .UseRabbitMqStreams(options =>
                {
                    options.StreamName = name;
                    options.StreamSystemConfig = CreateConfig();
                    options.LastPositionTimeout = System.TimeSpan.FromSeconds(10);
                })
                .WithDeduplication(true);

            async Task<List<int>> IdsAsync()
            {
                var stats = await system.StreamStats(name);

                try
                {
                    // at least up to the first message of the last chunk
                    return await ReadIdsAsync(system, name, (int)(stats.CommittedChunkId() - stats.FirstOffset()) + 1);
                }
                catch (OffsetNotFoundException)
                {
                    return [];
                }
            }

            try
            {
                await using (new RunningListener(CreateBuilder($"{name}_slot")))
                {
                    // one transaction each, so that the messages go in many chunks
                    for (var i = 1; i <= 500; i++) await Db.ExecuteAsync($"INSERT INTO {name} VALUES ({i}, 'a')");

                    await Wait.UntilAsync(async () => (await IdsAsync()).Contains(500), "500 in the stream", 60);
                }

                var stats = await system.StreamStats(name);

                Assert.IsTrue(stats.FirstOffset() > 0, "retention removed nothing");

                await Db.ExecuteAsync($"INSERT INTO {name} VALUES (501, 'b')");

                await using (new RunningListener(CreateBuilder($"{name}_replay_slot")))
                {
                    await Wait.UntilAsync(async () => (await IdsAsync()).Contains(501), "501 in the stream", 60);
                }

                // what retention left, then 501: nothing replayed
                var ids = await IdsAsync();

                CollectionAssert.AreEqual(Enumerable.Range(ids[0], 502 - ids[0]).ToList(), ids, string.Join(" ", ids));
            }
            finally
            {
                await system.Close();
            }
        }

        [TestMethod]
        [DataRow(true, false, "super stream")]
        [DataRow(false, true, "initial data copy")]
        public void Deduplication_should_not_build_with_a_super_stream_or_the_initial_copy(bool superStream, bool copy, string expected)
        {
            var builder = PgOutput2JsonBuilder.Create()
                .WithPgConnectionString("Host=localhost")
                .WithPgPublications("p")
                .WithPgReplicationSlot("s")
                .WithInitialDataCopy(copy)
                .UseRabbitMqStreams(options => options.IsSuperStream = superStream)
                .WithDeduplication(true);

            var e = Assert.ThrowsExactly<System.InvalidOperationException>(() => builder.Build());

            StringAssert.Contains(e.Message, expected);
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
