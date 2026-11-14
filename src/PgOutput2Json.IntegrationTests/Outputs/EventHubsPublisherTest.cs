using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;

using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace PgOutput2Json.IntegrationTests.Outputs
{
    /// <summary>
    /// Runs against the Event Hubs emulator of Microsoft. Its event hubs are listed in <see cref="Containers.EventHubs"/>.
    /// </summary>
    [TestClass]
    public class EventHubsPublisherTest
    {
        [TestMethod]
        public async Task Event_hub_should_get_the_changes_with_the_key_and_the_position()
        {
            var connectionString = await Containers.GetEventHubsConnectionStringAsync();

            var events = new List<EventData>();

            await MessageScenario.RunAsync("eh_plain",
                builder => builder.UseEventHubs(options =>
                {
                    options.ConnectionString = connectionString;
                    options.EventHubName = "eh_plain";
                }),
                async () => (events = (await ReadAsync(connectionString, "eh_plain")).Select(e => e.Event).ToList())
                    .Select(e => e.EventBody.ToString())
                    .ToList());

            Assert.AreEqual("public.eh_plain[1] public.eh_plain[2] public.eh_plain[1] public.eh_plain[2]", string.Join(" ", events.Select(e => e.MessageId)));

            // the scenario is one transaction, so the events share the position and differ in the message number
            Assert.AreEqual(1, events.Select(e => e.Properties["txFinalLsn"]).Distinct().Count());
            Assert.AreEqual("1 2 3 4", string.Join(" ", events.Select(e => e.Properties["messageNo"])));

            foreach (var e in events)
            {
                Assert.AreEqual("public.eh_plain", e.Properties["table"]);

                using var json = JsonDocument.Parse(e.EventBody.ToString());

                Assert.AreEqual(json.RootElement.GetProperty("w").GetUInt64(), Convert.ToUInt64(e.Properties["txFinalLsn"]));
            }
        }

        [TestMethod]
        public async Task Without_partition_key_columns_a_table_should_stay_in_one_partition_in_order()
        {
            var events = await PublishToPartitionsAsync("eh_route_table", builder => builder);

            Assert.AreEqual(1, events.Select(e => e.Partition).Distinct().Count(), "the rows of one table are in several partitions");

            CollectionAssert.AreEqual(Enumerable.Range(1, RouteRows).ToList(), events.Select(e => e.Id).ToList());
        }

        [TestMethod]
        public async Task Partition_key_columns_should_route_by_murmur2_of_the_partition_key()
        {
            var events = await PublishToPartitionsAsync("eh_route_pk", builder => builder.WithPartitionKeyFields("public.eh_route_pk", "v"));

            Assert.IsTrue(events.Select(e => e.Partition).Distinct().Count() > 1, "all the events went to one partition");

            foreach (var e in events)
            {
                // the partition key is written like the message key: the values in a JSON array
                Assert.AreEqual(ExpectedPartition($"[\"{e.V}\"]"), e.Partition, $"partition key {e.V}");
            }

            // every partition has its rows in the order of the changes
            foreach (var partition in events.GroupBy(e => e.Partition))
            {
                var ids = partition.Select(e => e.Id).ToList();

                CollectionAssert.AreEqual(ids.OrderBy(id => id).ToList(), ids, $"partition {partition.Key}");
            }
        }

        private const int RoutePartitions = 4;
        private const int RouteRows = 40;

        private static string ExpectedPartition(string key) => ((MurmurHash2.Hash(key) & 0x7fffffff) % RoutePartitions).ToString();

        private static async Task<List<(string Partition, int Id, string V)>> PublishToPartitionsAsync(string name, Func<PgOutput2JsonBuilder, PgOutput2JsonBuilder> configure)
        {
            var connectionString = await Containers.GetEventHubsConnectionStringAsync();

            await Db.ExecuteAsync($@"
                CREATE TABLE {name} (id int PRIMARY KEY, v text);
                CREATE PUBLICATION {name}_pub FOR TABLE {name};");

            await Db.CreateSlotAsync($"{name}_slot");

            var builder = configure(PgOutput2JsonBuilder.Create()
                .WithPgConnectionString(Containers.PgConnectionString)
                .WithPgPublications($"{name}_pub")
                .WithPgReplicationSlot($"{name}_slot")
                .WithBatchSize(7) // several sends, so the order between them counts too
                .UseEventHubs(options =>
                {
                    options.ConnectionString = connectionString;
                    options.EventHubName = name;
                }));

            var events = new List<(string Partition, EventData Event)>();

            await using (new RunningListener(builder))
            {
                await Db.ExecuteAsync($"INSERT INTO {name} SELECT i, 'v' || (i % 7) FROM generate_series(1, {RouteRows}) i");

                await Wait.UntilAsync(async () => (events = await ReadAsync(connectionString, name)).Count >= RouteRows, $"{RouteRows} events in the event hub", 60);
            }

            Assert.AreEqual(RouteRows, events.Count);

            return events.Select(e =>
            {
                using var json = JsonDocument.Parse(e.Event.EventBody.ToString());

                var row = json.RootElement.GetProperty("r");

                return (e.Partition, row.GetProperty("id").GetInt32(), row.GetProperty("v").GetString()!);
            }).ToList();
        }

        /// <summary>
        /// All the events of the event hub, partition by partition, in the order of each partition.
        /// </summary>
        public static async Task<List<(string Partition, EventData Event)>> ReadAsync(string connectionString, string eventHubName, EventHubConsumerClientOptions? clientOptions = null)
        {
            await using var consumer = new EventHubConsumerClient("test", connectionString, eventHubName, clientOptions);

            var events = new List<(string, EventData)>();

            foreach (var partitionId in (await consumer.GetPartitionIdsAsync()).OrderBy(int.Parse))
            {
                var properties = await consumer.GetPartitionPropertiesAsync(partitionId);

                if (properties.IsEmpty) continue;

                var options = new ReadEventOptions { MaximumWaitTime = TimeSpan.FromSeconds(5) };

                await foreach (var partitionEvent in consumer.ReadEventsFromPartitionAsync(partitionId, EventPosition.Earliest, options))
                {
                    // nothing arrived in the wait time
                    if (partitionEvent.Data == null) break;

                    events.Add((partitionId, partitionEvent.Data));

                    if (partitionEvent.Data.SequenceNumber >= properties.LastEnqueuedSequenceNumber) break;
                }
            }

            return events;
        }
    }
}
