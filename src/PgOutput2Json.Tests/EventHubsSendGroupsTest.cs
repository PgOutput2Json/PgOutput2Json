using System.Collections.Generic;
using System.Linq;

using Azure.Messaging.EventHubs;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using PgOutput2Json.AzureEventHubs;

namespace PgOutput2Json.Tests
{
    [TestClass]
    public class EventHubsSendGroupsTest
    {
        [TestMethod]
        public void GetSendGroups_should_keep_wal_order_within_a_client_resolved_partition()
        {
            // two tables interleaved in the same partition - grouping by table would send 1,3,5 before 2,4
            var buffer = new List<EventHubsPublisher.BufferEntry>
            {
                Entry("0", "a", 1),
                Entry("0", "b", 2),
                Entry("1", "a", 3),
                Entry("0", "a", 4),
                Entry("0", "b", 5),
            };

            var groups = Describe(EventHubsPublisher.GetSendGroups(buffer));

            CollectionAssert.AreEqual(new[] { "0:1,2,4,5", "1:3" }, groups);
        }

        [TestMethod]
        public void GetSendGroups_should_send_in_buffer_order_when_the_service_picks_the_partition()
        {
            // tables a and b can share a service-chosen partition, so their order must be kept
            var buffer = new List<EventHubsPublisher.BufferEntry>
            {
                Entry(null, "a", 1),
                Entry(null, "a", 2),
                Entry(null, "b", 3),
                Entry(null, "a", 4),
            };

            var groups = Describe(EventHubsPublisher.GetSendGroups(buffer));

            CollectionAssert.AreEqual(new[] { "a:1,2", "b:3", "a:4" }, groups);
        }

        [TestMethod]
        public void GetSendGroups_should_send_in_buffer_order_when_only_some_events_have_a_partition()
        {
            var buffer = new List<EventHubsPublisher.BufferEntry>
            {
                Entry("0", "a", 1),
                Entry(null, "b", 2),
                Entry("0", "a", 3),
                Entry("0", "c", 4),
            };

            var groups = Describe(EventHubsPublisher.GetSendGroups(buffer));

            CollectionAssert.AreEqual(new[] { "0:1", "b:2", "0:3,4" }, groups);
        }

        private static EventHubsPublisher.BufferEntry Entry(string? partitionId, string tableName, ulong messageNo)
        {
            return new EventHubsPublisher.BufferEntry(new EventData("{}"), partitionId, tableName, new WalPosition(100, messageNo));
        }

        // "target:messageNo,messageNo" per group, in send order
        private static string[] Describe(IEnumerable<List<EventHubsPublisher.BufferEntry>> groups)
        {
            return groups
                .Select(g => $"{g[0].PartitionId ?? g[0].PartitionKey}:{string.Join(",", g.Select(x => x.Position.MessageNo))}")
                .ToArray();
        }
    }
}
