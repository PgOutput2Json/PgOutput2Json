using System.Linq;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace PgOutput2Json.IntegrationTests
{
    [TestClass]
    public class LogicalMessageTest
    {
        [TestMethod]
        public async Task Non_transactional_message_should_have_its_own_deduplication_key()
        {
            await Db.ExecuteAsync(@"
                CREATE TABLE lm_table (id int PRIMARY KEY);
                CREATE PUBLICATION lm_pub FOR TABLE lm_table;");

            await Db.CreateSlotAsync("lm_slot");

            var publisher = new CollectingPublisherFactory();

            PgOutput2JsonBuilder CreateBuilder() => PgOutput2JsonBuilder.Create()
                .WithPgConnectionString(Containers.PgConnectionString)
                .WithPgPublications("lm_pub")
                .WithPgReplicationSlot("lm_slot")
                .WithMessagePublisherFactory(publisher);

            // sent outside of Begin/Commit - it used to inherit the key of the previous transaction
            await using (new RunningListener(CreateBuilder()))
            {
                await Db.ExecuteAsync("INSERT INTO lm_table VALUES (1)");
                await Db.ExecuteAsync("SELECT pg_logical_emit_message(false, 'app', 'after-transaction')");

                await Wait.UntilAsync(() => publisher.Messages.Count >= 2, "2 messages");
                await Wait.UntilAsync(() => publisher.LastConfirmed.TxFinalLsn == publisher.Messages[1].TxFinalLsn, "the confirm");
            }

            var row = publisher.Messages[0];
            var message = publisher.Messages[1];

            StringAssert.Contains(message.Json, "\"d\":\"after-transaction\"");
            Assert.IsTrue(message.TxFinalLsn > row.TxFinalLsn, "the message must be positioned after the previous transaction");
            Assert.AreEqual(1UL, message.MessageNo);

            // first message after a restart - it used to get the key 0/1 and be skipped as a duplicate
            await Db.ExecuteAsync("SELECT pg_logical_emit_message(false, 'app', 'first-after-restart')");
            await Db.ExecuteAsync("INSERT INTO lm_table VALUES (2)");

            await using (new RunningListener(CreateBuilder()))
            {
                await Wait.UntilAsync(() => publisher.Messages.Count >= 4, "4 messages");
            }

            var published = publisher.Messages;

            Assert.AreEqual(4, published.Count, "nothing may be published twice");
            StringAssert.Contains(published[2].Json, "\"d\":\"first-after-restart\"");
            Assert.IsTrue(published[2].TxFinalLsn > message.TxFinalLsn);
            Assert.IsTrue(published[3].TxFinalLsn > published[2].TxFinalLsn);
            Assert.AreEqual(4, published.Select(x => (x.TxFinalLsn, x.MessageNo)).Distinct().Count(), "every message needs a unique key");
        }
    }
}
