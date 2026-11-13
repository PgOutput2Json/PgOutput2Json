using System.Linq;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace PgOutput2Json.IntegrationTests
{
    [TestClass]
    public class PartitionKeyTest
    {
        [TestMethod]
        public async Task Partition_key_should_be_the_same_for_insert_update_and_delete()
        {
            // with REPLICA IDENTITY FULL an update carries the old and the new row -
            // the partition key used to be written from both, as [7,7]
            await Db.ExecuteAsync(@"
                CREATE TABLE pk_default (id int PRIMARY KEY, tenant int, v text);
                CREATE TABLE pk_full (id int PRIMARY KEY, tenant int, v text);
                ALTER TABLE pk_full REPLICA IDENTITY FULL;
                CREATE PUBLICATION pk_pub FOR TABLE pk_default, pk_full;");

            await Db.CreateSlotAsync("pk_slot");

            var publisher = new CollectingPublisherFactory();

            var builder = PgOutput2JsonBuilder.Create()
                .WithPgConnectionString(Containers.PgConnectionString)
                .WithPgPublications("pk_pub")
                .WithPgReplicationSlot("pk_slot")
                .WithPartitionKeyFields("public.pk_default", "id")
                .WithPartitionKeyFields("public.pk_full", "tenant")
                .WithMessagePublisherFactory(publisher);

            await using (new RunningListener(builder))
            {
                foreach (var table in new[] { "pk_default", "pk_full" })
                {
                    await Db.ExecuteAsync($"INSERT INTO {table} VALUES (1, 7, 'a'); UPDATE {table} SET v = 'b' WHERE id = 1; DELETE FROM {table} WHERE id = 1;");
                }

                await Wait.UntilAsync(() => publisher.Messages.Count >= 6, "6 messages");
            }

            var partitionKeys = string.Join(" ", publisher.Messages.Select(x => $"{x.Table}:{x.PartitionKey}"));

            Assert.AreEqual(
                "public.pk_default:[1] public.pk_default:[1] public.pk_default:[1] public.pk_full:[7] public.pk_full:[7] public.pk_full:[7]",
                partitionKeys);
        }

        [TestMethod]
        public async Task Partition_key_column_outside_the_replica_identity_should_log_an_error()
        {
            await Db.ExecuteAsync(@"
                CREATE TABLE pk_outside (id int PRIMARY KEY, tenant int);
                CREATE PUBLICATION pk_outside_pub FOR TABLE pk_outside;");

            await Db.CreateSlotAsync("pk_outside_slot");

            var publisher = new CollectingPublisherFactory();
            using var logs = new CollectingLoggerFactory();

            var builder = PgOutput2JsonBuilder.Create()
                .WithLoggerFactory(logs)
                .WithPgConnectionString(Containers.PgConnectionString)
                .WithPgPublications("pk_outside_pub")
                .WithPgReplicationSlot("pk_outside_slot")
                .WithPartitionKeyFields("public.pk_outside", "tenant")
                .WithMessagePublisherFactory(publisher);

            await using (new RunningListener(builder))
            {
                await Db.ExecuteAsync("INSERT INTO pk_outside VALUES (1, 7); DELETE FROM pk_outside WHERE id = 1;");

                await Wait.UntilAsync(() => publisher.Messages.Count >= 2, "2 messages");
            }

            Assert.IsTrue(logs.Contains(LogLevel.Error, "Partition key column tenant of table public.pk_outside is not part of the replica identity"));

            // the limitation the error is about: the delete carries no partition key
            CollectionAssert.AreEqual(new[] { "[7]", "" }, publisher.Messages.Select(x => x.PartitionKey).ToArray());
        }
    }
}
