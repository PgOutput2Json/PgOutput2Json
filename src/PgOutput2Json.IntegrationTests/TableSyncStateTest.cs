using System.Linq;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Npgsql;

namespace PgOutput2Json.IntegrationTests
{
    [TestClass]
    public class TableSyncStateTest
    {
        private static PgOutput2JsonBuilder CreateBuilder(string name, CollectingPublisherFactory publisher, bool copyData)
        {
            return PgOutput2JsonBuilder.Create()
                .WithPgConnectionString(Containers.PgConnectionString)
                .WithPgPublications($"{name}_pub")
                .WithPgReplicationSlot($"{name}_slot")
                .WithInitialDataCopy(copyData)
                .WithJsonOptions(options => options.WriteTableNames = true)
                .WithMessagePublisherFactory(publisher);
        }

        private static string Describe(CollectingPublisherFactory publisher)
        {
            return string.Join(" ", publisher.Messages.Select(m => $"{m.Table}{m.Key}"));
        }

        [TestMethod]
        public async Task Copy_off_should_register_the_tables_so_only_new_ones_are_copied_later()
        {
            await Db.ExecuteAsync(@"
                CREATE TABLE tss_off_a (id int PRIMARY KEY);
                CREATE TABLE tss_off_b (id int PRIMARY KEY);
                INSERT INTO tss_off_a VALUES (1);
                INSERT INTO tss_off_b VALUES (2);
                CREATE PUBLICATION tss_off_pub FOR TABLE tss_off_a;");

            await Db.CreateSlotAsync("tss_off_slot");

            var publisher = new CollectingPublisherFactory();

            await using (new RunningListener(CreateBuilder("tss_off", publisher, copyData: false)))
            {
                await Wait.UntilAsync(async () => await Db.TableStatesAsync("tss_off_slot") == "public.tss_off_a:ready", "the table state");
            }

            Assert.AreEqual(0, publisher.Messages.Count);

            await Db.ExecuteAsync("ALTER PUBLICATION tss_off_pub ADD TABLE tss_off_b");

            // the copy is enabled later: only the table added since is copied
            await using (new RunningListener(CreateBuilder("tss_off", publisher, copyData: true)))
            {
                await Wait.UntilAsync(async () => await Db.TableStatesAsync("tss_off_slot") == "public.tss_off_a:ready,public.tss_off_b:ready", "both tables ready");
            }

            Assert.AreEqual("public.tss_off_b[2]", Describe(publisher));
        }

        [TestMethod]
        public async Task Copy_on_should_copy_a_table_once_and_a_table_published_later_on_the_next_start()
        {
            await Db.ExecuteAsync(@"
                CREATE TABLE tss_on_a (id int PRIMARY KEY);
                CREATE TABLE tss_on_b (id int PRIMARY KEY);
                INSERT INTO tss_on_a VALUES (1), (2);
                INSERT INTO tss_on_b VALUES (3);
                CREATE PUBLICATION tss_on_pub FOR TABLE tss_on_a;");

            await Db.CreateSlotAsync("tss_on_slot");

            var publisher = new CollectingPublisherFactory();

            await using (new RunningListener(CreateBuilder("tss_on", publisher, copyData: true)))
            {
                await Wait.UntilAsync(async () => await Db.TableStatesAsync("tss_on_slot") == "public.tss_on_a:ready", "the table state");
            }

            Assert.AreEqual("public.tss_on_a[1] public.tss_on_a[2]", Describe(publisher));

            // nothing is copied again
            await using (new RunningListener(CreateBuilder("tss_on", publisher, copyData: true)))
            {
                await Db.ExecuteAsync("INSERT INTO tss_on_a VALUES (4)");
                await Wait.UntilAsync(() => publisher.Messages.Count >= 3, "the streamed row");

                // an unconfirmed row would be replayed by the next start
                await Wait.UntilAsync(() => publisher.LastConfirmed.TxFinalLsn == publisher.Messages[2].TxFinalLsn, "the confirm");
            }

            Assert.AreEqual("public.tss_on_a[1] public.tss_on_a[2] public.tss_on_a[4]", Describe(publisher));

            await Db.ExecuteAsync("ALTER PUBLICATION tss_on_pub ADD TABLE tss_on_b");

            await using (new RunningListener(CreateBuilder("tss_on", publisher, copyData: true)))
            {
                await Wait.UntilAsync(async () => await Db.TableStatesAsync("tss_on_slot") == "public.tss_on_a:ready,public.tss_on_b:ready", "both tables ready");
            }

            Assert.AreEqual("public.tss_on_a[1] public.tss_on_a[2] public.tss_on_a[4] public.tss_on_b[3]", Describe(publisher));
        }

        [TestMethod]
        public async Task Table_removed_from_the_publication_or_created_again_should_be_copied_again()
        {
            await Db.ExecuteAsync(@"
                CREATE TABLE tss_re_a (id int PRIMARY KEY);
                CREATE TABLE tss_re_b (id int PRIMARY KEY);
                INSERT INTO tss_re_a VALUES (1);
                INSERT INTO tss_re_b VALUES (2);
                CREATE PUBLICATION tss_re_pub FOR TABLE tss_re_a, tss_re_b;");

            await Db.CreateSlotAsync("tss_re_slot");

            var publisher = new CollectingPublisherFactory();

            await using (new RunningListener(CreateBuilder("tss_re", publisher, copyData: true)))
            {
                await Wait.UntilAsync(async () => await Db.TableStatesAsync("tss_re_slot") == "public.tss_re_a:ready,public.tss_re_b:ready", "both tables ready");
            }

            Assert.AreEqual(2, publisher.Messages.Count);

            await Db.ExecuteAsync("ALTER PUBLICATION tss_re_pub DROP TABLE tss_re_a");

            await using (new RunningListener(CreateBuilder("tss_re", publisher, copyData: true)))
            {
                await Wait.UntilAsync(async () => await Db.TableStatesAsync("tss_re_slot") == "public.tss_re_b:ready", "the state of the removed table to go");
            }

            // a is published again, b is a new table under the old name
            await Db.ExecuteAsync(@"
                ALTER PUBLICATION tss_re_pub ADD TABLE tss_re_a;
                DROP TABLE tss_re_b;
                CREATE TABLE tss_re_b (id int PRIMARY KEY);
                INSERT INTO tss_re_b VALUES (20);
                ALTER PUBLICATION tss_re_pub ADD TABLE tss_re_b;");

            await using (new RunningListener(CreateBuilder("tss_re", publisher, copyData: true)))
            {
                await Wait.UntilAsync(() => publisher.Messages.Count >= 4, "both tables copied again");
                await Wait.UntilAsync(async () => await Db.TableStatesAsync("tss_re_slot") == "public.tss_re_a:ready,public.tss_re_b:ready", "both tables ready");
            }

            CollectionAssert.AreEquivalent(
                new[] { "public.tss_re_a[1]", "public.tss_re_b[20]" },
                publisher.Messages.Skip(2).Select(m => $"{m.Table}{m.Key}").ToArray());
        }

        [TestMethod]
        public async Task Tables_copied_before_the_upgrade_should_not_be_copied_again()
        {
            await Db.ExecuteAsync(@"
                CREATE TABLE tss_mig_a (id int PRIMARY KEY);
                CREATE TABLE tss_mig_b (id int PRIMARY KEY);
                INSERT INTO tss_mig_a VALUES (1);
                INSERT INTO tss_mig_b VALUES (2);
                CREATE PUBLICATION tss_mig_pub FOR TABLE tss_mig_a, tss_mig_b;

                CREATE SCHEMA IF NOT EXISTS pgoutput2json;
                CREATE TABLE IF NOT EXISTS pgoutput2json.data_copy_progress (
                    table_name TEXT NOT NULL,
                    slot_name TEXT NOT NULL,
                    is_completed bool NOT NULL DEFAULT false,
                    last_message jsonb,
                    column_names jsonb,
                    created_at timestamptz DEFAULT now(),
                    updated_at timestamptz DEFAULT now(),
                    CONSTRAINT pk_data_copy_progress PRIMARY KEY (table_name, slot_name)
                );
                INSERT INTO pgoutput2json.data_copy_progress (table_name, slot_name, is_completed)
                VALUES ('public.tss_mig_a', 'tss_mig_slot', true);");

            await Db.CreateSlotAsync("tss_mig_slot");

            var publisher = new CollectingPublisherFactory();

            await using (new RunningListener(CreateBuilder("tss_mig", publisher, copyData: true)))
            {
                await Wait.UntilAsync(async () => await Db.TableStatesAsync("tss_mig_slot") == "public.tss_mig_a:ready,public.tss_mig_b:ready", "both tables ready");
            }

            Assert.AreEqual("public.tss_mig_b[2]", Describe(publisher));
        }

        [TestMethod]
        public async Task Table_in_two_publications_should_be_copied_once_with_either_row_filter()
        {
            Containers.RequirePg(15, "Row filters");

            await Db.ExecuteAsync(@"
                CREATE TABLE tss_two (id int PRIMARY KEY);
                INSERT INTO tss_two VALUES (1), (2), (3), (4);
                CREATE PUBLICATION tss_two_pub FOR TABLE tss_two WHERE (id < 2);
                CREATE PUBLICATION tss_two_pub2 FOR TABLE tss_two WHERE (id > 3);");

            await Db.CreateSlotAsync("tss_two_slot");

            var publisher = new CollectingPublisherFactory();

            var builder = CreateBuilder("tss_two", publisher, copyData: true).WithPgPublications("tss_two_pub", "tss_two_pub2");

            await using (new RunningListener(builder))
            {
                await Wait.UntilAsync(async () => await Db.TableStatesAsync("tss_two_slot") == "public.tss_two:ready", "the table state");
            }

            Assert.AreEqual("public.tss_two[1] public.tss_two[4]", Describe(publisher));
        }

        [TestMethod]
        public async Task Copy_off_should_keep_streaming_when_the_state_cannot_be_stored()
        {
            await Db.ExecuteAsync(@"
                CREATE TABLE tss_priv (id int PRIMARY KEY);
                CREATE PUBLICATION tss_priv_pub FOR TABLE tss_priv;
                CREATE ROLE tss_priv_user LOGIN REPLICATION PASSWORD 'secret';
                CREATE SCHEMA IF NOT EXISTS pgoutput2json;");

            await Db.CreateSlotAsync("tss_priv_slot");

            using var logs = new CollectingLoggerFactory();

            var publisher = new CollectingPublisherFactory();

            var connectionString = new NpgsqlConnectionStringBuilder(Containers.PgConnectionString) { Username = "tss_priv_user", Password = "secret" }.ConnectionString;

            var builder = CreateBuilder("tss_priv", publisher, copyData: false)
                .WithPgConnectionString(connectionString)
                .WithLoggerFactory(logs);

            await using (new RunningListener(builder))
            {
                await Db.ExecuteAsync("INSERT INTO tss_priv VALUES (1)");

                await Wait.UntilAsync(() => publisher.Messages.Count >= 1, "the streamed row");
            }

            Assert.IsTrue(logs.Contains(LogLevel.Warning, "Could not store the table states"));
            Assert.AreEqual("", await Db.TableStatesAsync("tss_priv_slot"));
        }

        [TestMethod]
        public async Task Own_tables_should_not_be_published_with_a_publication_for_all_tables()
        {
            await Db.ExecuteAsync(@"
                CREATE TABLE tss_all (id int PRIMARY KEY);
                CREATE PUBLICATION tss_all_pub FOR ALL TABLES;");

            await Db.CreateSlotAsync("tss_all_slot");

            var publisher = new CollectingPublisherFactory();

            await using (new RunningListener(CreateBuilder("tss_all", publisher, copyData: false)))
            {
                await Wait.UntilAsync(async () => (await Db.TableStatesAsync("tss_all_slot")).Contains("public.tss_all:ready"), "the table state");

                // written after the slot was created, so it is in the stream
                await Db.ExecuteAsync("UPDATE pgoutput2json.table_sync_state SET updated_at = now() WHERE slot_name = 'tss_all_slot'");
                await Db.ExecuteAsync("INSERT INTO tss_all VALUES (1)");

                await Wait.UntilAsync(() => publisher.Messages.Any(m => m.Table == "public.tss_all"), "the streamed row");
            }

            Assert.IsFalse((await Db.TableStatesAsync("tss_all_slot")).Contains("pgoutput2json."), "own tables must not be registered");
            Assert.IsFalse(publisher.Messages.Any(m => m.Table.StartsWith("pgoutput2json.")), Describe(publisher));
        }
    }
}
