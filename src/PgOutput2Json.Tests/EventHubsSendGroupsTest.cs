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

            CollectionAssert.AreEqual(new[] { "0:a1,b2,a4,b5", "1:a3" }, groups);
        }

        private static EventHubsPublisher.BufferEntry Entry(string partitionId, string tableName, ulong messageNo)
        {
            return new EventHubsPublisher.BufferEntry(new EventData("{}") { MessageId = $"{tableName}{messageNo}" }, partitionId);
        }

        // "target:messageNo,messageNo" per group, in send order
        private static string[] Describe(IEnumerable<List<EventHubsPublisher.BufferEntry>> groups)
        {
            return groups
                .Select(g => $"{g[0].PartitionId}:{string.Join(",", g.Select(x => x.EventData.MessageId))}")
                .ToArray();
        }
    }
}
