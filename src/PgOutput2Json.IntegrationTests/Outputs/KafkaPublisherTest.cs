using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NpgsqlTypes;

using PgOutput2Json.Kafka;

namespace PgOutput2Json.IntegrationTests.Outputs
{
    [TestClass]
    public class KafkaPublisherTest
    {
        [TestMethod]
        public async Task Replay_should_publish_the_messages_again_in_order()
        {
            var bootstrapServers = await Containers.GetKafkaBootstrapServersAsync();

            await CreateTopicAsync(bootstrapServers, "kafka_restart", null);

            await Db.ExecuteAsync(@"
                CREATE TABLE kafka_restart (id int PRIMARY KEY, v text);
                CREATE PUBLICATION kafka_restart_pub FOR TABLE kafka_restart;");

            // the second slot stays at the start, so a listener on it gets every row again
            await Db.CreateSlotAsync("kafka_restart_slot");
            await Db.CreateSlotAsync("kafka_restart_replay_slot");

            PgOutput2JsonBuilder CreateBuilder(string slotName) => PgOutput2JsonBuilder.Create()
                .WithPgConnectionString(Containers.PgConnectionString)
                .WithPgPublications("kafka_restart_pub")
                .WithPgReplicationSlot(slotName)
                .UseKafka(options =>
                {
                    options.Topic = "kafka_restart";
                    options.ProducerConfig.BootstrapServers = bootstrapServers;
                });

            await using (new RunningListener(CreateBuilder("kafka_restart_slot")))
            {
                await Db.ExecuteAsync("INSERT INTO kafka_restart VALUES (1, 'a'), (2, 'b')");
                await Db.ExecuteAsync("INSERT INTO kafka_restart VALUES (3, 'c')");

                await Wait.UntilAsync(() => ReadIds(bootstrapServers, "kafka_restart").Count >= 3, "3 messages in the topic", 60);
            }

            await Db.ExecuteAsync("INSERT INTO kafka_restart VALUES (4, 'd')");

            // delivery is at-least-once: the first three rows are replayed and published again, consumers deduplicate
            await using (new RunningListener(CreateBuilder("kafka_restart_replay_slot")))
            {
                await Wait.UntilAsync(() => ReadIds(bootstrapServers, "kafka_restart").Count >= 7, "7 messages in the topic", 60);
            }

            CollectionAssert.AreEqual(new[] { 1, 2, 3, 1, 2, 3, 4 }, ReadIds(bootstrapServers, "kafka_restart"));
        }

        [TestMethod]
        public async Task Message_key_should_route_like_the_murmur2_partitioner()
        {
            var records = await PublishToPartitionsAsync("kafka_route_key", builder => builder);

            foreach (var record in records)
            {
                Assert.AreEqual(ExpectedPartition(record.Message.Key), record.Partition.Value, $"key {record.Message.Key}");
            }
        }

        [TestMethod]
        public async Task Partition_key_columns_should_route_by_murmur2_of_the_partition_key()
        {
            var records = await PublishToPartitionsAsync("kafka_route_pk", builder => builder.WithPartitionKeyFields("public.kafka_route_pk", "v"));

            foreach (var record in records)
            {
                var partitionKey = Encoding.UTF8.GetString(record.Message.Headers.GetLastBytes("partition_key"));

                Assert.AreNotEqual(record.Message.Key, partitionKey);
                Assert.AreEqual(ExpectedPartition(partitionKey), record.Partition.Value, $"partition key {partitionKey}");
            }
        }

        private const int RoutePartitions = 4;
        private const int RouteRows = 40;

        private static int ExpectedPartition(string key) => (MurmurHash2.Hash(key) & 0x7fffffff) % RoutePartitions;

        /// <summary>
        /// Publishes rows to a topic with several partitions and returns the records of all the partitions.
        /// </summary>
        private static async Task<List<ConsumeResult<string, string>>> PublishToPartitionsAsync(string name, Func<PgOutput2JsonBuilder, PgOutput2JsonBuilder> configure)
        {
            var bootstrapServers = await Containers.GetKafkaBootstrapServersAsync();

            await CreateTopicAsync(bootstrapServers, name, null, RoutePartitions);

            await Db.ExecuteAsync($@"
                CREATE TABLE {name} (id int PRIMARY KEY, v text);
                ALTER TABLE {name} REPLICA IDENTITY FULL;
                CREATE PUBLICATION {name}_pub FOR TABLE {name};");

            await Db.CreateSlotAsync($"{name}_slot");

            var builder = configure(PgOutput2JsonBuilder.Create()
                .WithPgConnectionString(Containers.PgConnectionString)
                .WithPgPublications($"{name}_pub")
                .WithPgReplicationSlot($"{name}_slot")
                .UseKafka(options =>
                {
                    options.Topic = name;
                    options.WriteHeaders = true;
                    options.ProducerConfig.BootstrapServers = bootstrapServers;
                }));

            await using (new RunningListener(builder))
            {
                await Db.ExecuteAsync($"INSERT INTO {name} SELECT i, 'v' || (i % 7) FROM generate_series(1, {RouteRows}) i");

                await Wait.UntilAsync(() => ReadAll(bootstrapServers, name, RoutePartitions).Count >= RouteRows, $"{RouteRows} messages in the topic", 60);
            }

            var records = ReadAll(bootstrapServers, name, RoutePartitions);

            Assert.AreEqual(RouteRows, records.Count);
            Assert.IsTrue(records.Select(r => r.Partition.Value).Distinct().Count() > 1, "all the messages went to one partition");

            return records;
        }

        [TestMethod]
        public async Task Rejected_message_should_fail_the_confirm()
        {
            var bootstrapServers = await Containers.GetKafkaBootstrapServersAsync();

            // the broker rejects anything larger - the delivery fails, and it used to be confirmed anyway
            await CreateTopicAsync(bootstrapServers, "kafka_rejected", new Dictionary<string, string> { ["max.message.bytes"] = "1000" });

            await Db.ExecuteAsync(@"
                CREATE TABLE kafka_rejected (id int PRIMARY KEY, v text);
                CREATE PUBLICATION kafka_rejected_pub FOR TABLE kafka_rejected;");

            await Db.CreateSlotAsync("kafka_rejected_slot");

            using var logs = new CollectingLoggerFactory();

            var builder = PgOutput2JsonBuilder.Create()
                .WithLoggerFactory(logs)
                .WithPgConnectionString(Containers.PgConnectionString)
                .WithPgPublications("kafka_rejected_pub")
                .WithPgReplicationSlot("kafka_rejected_slot")
                .UseKafka(options =>
                {
                    options.Topic = "kafka_rejected";
                    options.ProducerConfig.BootstrapServers = bootstrapServers;
                });

            NpgsqlLogSequenceNumber beforeRejected;

            await using (new RunningListener(builder))
            {
                await Db.ExecuteAsync("INSERT INTO kafka_rejected VALUES (1, 'a')");

                await Wait.UntilAsync(() => ReadIds(bootstrapServers, "kafka_rejected").Count >= 1, "1 message in the topic", 60);

                beforeRejected = await Db.ScalarAsync<NpgsqlLogSequenceNumber>("SELECT pg_current_wal_insert_lsn()");

                await Db.ExecuteAsync($"INSERT INTO kafka_rejected VALUES (2, '{new string('x', 5000)}')");

                await Wait.UntilAsync(() => logs.Contains(LogLevel.Error, "Kafka delivery to topic kafka_rejected failed"), "the delivery failure", 60);
            }

            var confirmed = await Db.ScalarAsync<NpgsqlLogSequenceNumber>(
                "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'kafka_rejected_slot'");

            Assert.IsTrue(confirmed <= beforeRejected, $"the rejected row was confirmed to PostgreSQL: {confirmed} > {beforeRejected}");
            CollectionAssert.AreEqual(new[] { 1 }, ReadIds(bootstrapServers, "kafka_rejected"));
        }

        private static async Task CreateTopicAsync(string bootstrapServers, string topic, Dictionary<string, string>? configs, int partitions = 1)
        {
            using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();

            await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = partitions, ReplicationFactor = 1, Configs = configs }]);
        }

        /// <summary>
        /// The row ids of all the messages in the single-partition topic, in the topic order.
        /// </summary>
        private static List<int> ReadIds(string bootstrapServers, string topic)
        {
            return ReadAll(bootstrapServers, topic, 1)
                .Select(r =>
                {
                    using var json = JsonDocument.Parse(r.Message.Value);

                    return json.RootElement.GetProperty("r").GetProperty("id").GetInt32();
                })
                .ToList();
        }

        /// <summary>
        /// All the records of the topic, partition by partition.
        /// </summary>
        private static List<ConsumeResult<string, string>> ReadAll(string bootstrapServers, string topic, int partitions)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = Guid.NewGuid().ToString(),
                EnableAutoCommit = false,
            };

            using var consumer = new ConsumerBuilder<string, string>(config).Build();

            var records = new List<ConsumeResult<string, string>>();

            for (var i = 0; i < partitions; i++)
            {
                var partition = new TopicPartition(topic, i);
                var end = consumer.QueryWatermarkOffsets(partition, TimeSpan.FromSeconds(10)).High;

                consumer.Assign(new TopicPartitionOffset(partition, Offset.Beginning));

                for (var read = 0; read < end.Value; read++)
                {
                    records.Add(consumer.Consume(TimeSpan.FromSeconds(10))
                        ?? throw new TimeoutException($"Could not read topic {topic}"));
                }
            }

            return records;
        }
    }
}
