using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;

using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NpgsqlTypes;

using PgOutput2Json.Kafka;

namespace PgOutput2Json.IntegrationTests
{
    [TestClass]
    public class KafkaPublisherTest
    {
        [TestMethod]
        public async Task Restart_should_not_publish_a_message_twice()
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

            // the first three rows are replayed, and must be skipped as already published
            await using (new RunningListener(CreateBuilder("kafka_restart_replay_slot")))
            {
                await Wait.UntilAsync(() => ReadIds(bootstrapServers, "kafka_restart").Count >= 4, "4 messages in the topic", 60);
            }

            CollectionAssert.AreEqual(new[] { 1, 2, 3, 4 }, ReadIds(bootstrapServers, "kafka_restart"));
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

                beforeRejected = await Db.ScalarAsync<NpgsqlLogSequenceNumber>("SELECT pg_current_wal_lsn()");

                await Db.ExecuteAsync($"INSERT INTO kafka_rejected VALUES (2, '{new string('x', 5000)}')");

                await Wait.UntilAsync(() => logs.Contains(LogLevel.Error, "Kafka delivery to topic kafka_rejected failed"), "the delivery failure", 60);
            }

            var confirmed = await Db.ScalarAsync<NpgsqlLogSequenceNumber>(
                "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'kafka_rejected_slot'");

            Assert.IsTrue(confirmed <= beforeRejected, $"the rejected row was confirmed to PostgreSQL: {confirmed} > {beforeRejected}");
            CollectionAssert.AreEqual(new[] { 1 }, ReadIds(bootstrapServers, "kafka_rejected"));
        }

        private static async Task CreateTopicAsync(string bootstrapServers, string topic, Dictionary<string, string>? configs)
        {
            using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();

            await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1, Configs = configs }]);
        }

        /// <summary>
        /// The row ids of all the messages in the single-partition topic, in the topic order.
        /// </summary>
        private static List<int> ReadIds(string bootstrapServers, string topic)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = Guid.NewGuid().ToString(),
                EnableAutoCommit = false,
            };

            using var consumer = new ConsumerBuilder<string, string>(config).Build();

            var partition = new TopicPartition(topic, 0);
            var end = consumer.QueryWatermarkOffsets(partition, TimeSpan.FromSeconds(10)).High;

            consumer.Assign(new TopicPartitionOffset(partition, Offset.Beginning));

            var ids = new List<int>();

            while (ids.Count < end.Value)
            {
                var result = consumer.Consume(TimeSpan.FromSeconds(10))
                    ?? throw new TimeoutException($"Could not read topic {topic}");

                using var json = JsonDocument.Parse(result.Message.Value);

                ids.Add(json.RootElement.GetProperty("r").GetProperty("id").GetInt32());
            }

            return ids;
        }
    }
}
