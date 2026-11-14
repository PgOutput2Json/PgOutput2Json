using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NpgsqlTypes;

namespace PgOutput2Json.IntegrationTests
{
    [TestClass]
    public class ListenerTest
    {
        private static PgOutput2JsonBuilder CreateBuilder(string name, CollectingPublisherFactory publisher)
        {
            return PgOutput2JsonBuilder.Create()
                .WithPgConnectionString(Containers.PgConnectionString)
                .WithPgPublications($"{name}_pub")
                .WithPgReplicationSlot($"{name}_slot")
                .WithMessagePublisherFactory(publisher);
        }

        private static async Task CreateAsync(string name, bool createSlot = true)
        {
            await Db.ExecuteAsync($@"
                CREATE TABLE {name} (id int PRIMARY KEY, v text);
                CREATE PUBLICATION {name}_pub FOR TABLE {name};");

            if (createSlot) await Db.CreateSlotAsync($"{name}_slot");
        }

        private static string Keys(CollectingPublisherFactory publisher)
        {
            return string.Join(" ", publisher.Messages.Select(m => m.Key));
        }

        private static Task<NpgsqlLogSequenceNumber> ConfirmedLsnAsync(string name)
        {
            return Db.ScalarAsync<NpgsqlLogSequenceNumber>($"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '{name}_slot'");
        }

        [TestMethod]
        public async Task Full_batch_should_be_confirmed_at_once_and_the_rest_after_the_wait_time()
        {
            await CreateAsync("ls_batch");

            var publisher = new CollectingPublisherFactory();

            await using (new RunningListener(CreateBuilder("ls_batch", publisher).WithBatchSize(3)))
            {
                // one transaction, so the rows arrive together
                await Db.ExecuteAsync("INSERT INTO ls_batch SELECT i, 'v' FROM generate_series(1, 7) i");

                var afterInsert = await Db.ScalarAsync<NpgsqlLogSequenceNumber>("SELECT pg_current_wal_insert_lsn()");

                await Wait.UntilAsync(() => publisher.ConfirmedBatches.Sum() == 7, "7 confirmed messages");

                CollectionAssert.AreEqual(new[] { 3, 3, 1 }, publisher.ConfirmedBatches);

                // confirmed to PostgreSQL too: the slot has moved past the transaction
                await Wait.UntilAsync(async () => await ConfirmedLsnAsync("ls_batch") >= afterInsert, "the slot to move");
            }
        }

        [TestMethod]
        public async Task Changes_made_while_stopped_should_be_published_on_the_next_start_and_only_once_if_confirmed()
        {
            await CreateAsync("ls_stop");

            var publisher = new CollectingPublisherFactory();

            await using (new RunningListener(CreateBuilder("ls_stop", publisher)))
            {
                await Db.ExecuteAsync("INSERT INTO ls_stop VALUES (1, 'a')");

                await Wait.UntilAsync(() => publisher.Messages.Count >= 1, "the first row");
                await Wait.UntilAsync(() => publisher.LastConfirmed.TxFinalLsn == publisher.Messages[0].TxFinalLsn, "the confirm");
            }

            await Db.ExecuteAsync("INSERT INTO ls_stop VALUES (2, 'b'); UPDATE ls_stop SET v = 'c' WHERE id = 1;");

            await using (new RunningListener(CreateBuilder("ls_stop", publisher)))
            {
                await Wait.UntilAsync(() => publisher.Messages.Count >= 3, "the rows written while stopped");
            }

            Assert.AreEqual("[1] [2] [1]", Keys(publisher));
        }

        [TestMethod]
        public async Task Temporary_slot_should_publish_only_the_changes_made_while_running()
        {
            await CreateAsync("ls_temp", createSlot: false);

            await Db.ExecuteAsync("INSERT INTO ls_temp VALUES (1, 'before')");

            var publisher = new CollectingPublisherFactory();

            var builder = CreateBuilder("ls_temp", publisher).WithPgReplicationSlot("ls_temp_slot", useTemporarySlot: true);

            await using (new RunningListener(builder))
            {
                await Wait.UntilAsync(async () => await Db.ScalarAsync<long>("SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'ls_temp_slot' AND temporary AND active") == 1, "the temporary slot");

                await Db.ExecuteAsync("INSERT INTO ls_temp VALUES (2, 'during')");

                await Wait.UntilAsync(() => publisher.Messages.Count >= 1, "the row");
            }

            Assert.AreEqual("[2]", Keys(publisher));

            await Wait.UntilAsync(async () => await Db.ScalarAsync<long>("SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'ls_temp_slot'") == 0, "the slot to go");
        }

        [TestMethod]
        public async Task Failed_publish_should_reconnect_and_publish_the_unconfirmed_changes_again()
        {
            await CreateAsync("ls_fail");

            using var logs = new CollectingLoggerFactory();

            var failed = false;

            var publisher = new CollectingPublisherFactory
            {
                // the third row fails once, after the first two were published but not confirmed
                OnPublish = message =>
                {
                    if (!failed && message.KeyKolValue.ToString() == "[3]")
                    {
                        failed = true;
                        throw new InvalidOperationException("The output is down");
                    }
                }
            };

            await using (new RunningListener(CreateBuilder("ls_fail", publisher).WithLoggerFactory(logs).WithReconnectDelay(TimeSpan.FromMilliseconds(500))))
            {
                await Db.ExecuteAsync("INSERT INTO ls_fail SELECT i, 'v' FROM generate_series(1, 4) i");

                await Wait.UntilAsync(() => publisher.Messages.Count >= 6, "the replay", 60);
                await Wait.UntilAsync(() => publisher.ConfirmedBatches.Sum() >= 4, "the confirm");
            }

            // at-least-once: the whole transaction again, in order, and nothing lost
            Assert.AreEqual("[1] [2] [1] [2] [3] [4]", Keys(publisher));
            Assert.IsTrue(logs.Contains(LogLevel.Error, "The output is down"));
            Assert.AreEqual(2, publisher.PublisherCount, "a new publisher is created for the new connection");

            // the replay has the same deduplication keys
            var messages = publisher.Messages;

            Assert.AreEqual((messages[0].TxFinalLsn, messages[0].MessageNo), (messages[2].TxFinalLsn, messages[2].MessageNo));
            Assert.AreEqual((messages[1].TxFinalLsn, messages[1].MessageNo), (messages[3].TxFinalLsn, messages[3].MessageNo));
        }

        [TestMethod]
        public async Task Failed_confirm_should_not_move_the_slot_and_should_publish_the_batch_again()
        {
            await CreateAsync("ls_cfail");

            using var logs = new CollectingLoggerFactory();

            var before = await ConfirmedLsnAsync("ls_cfail");
            var failed = false;
            var slotAtFailure = default(NpgsqlLogSequenceNumber);

            var publisher = new CollectingPublisherFactory();

            publisher.OnConfirm = () =>
            {
                if (failed || publisher.Messages.Count == 0) return;

                failed = true;
                throw new InvalidOperationException("The output did not acknowledge");
            };

            await using (new RunningListener(CreateBuilder("ls_cfail", publisher).WithLoggerFactory(logs).WithReconnectDelay(TimeSpan.FromMilliseconds(500))))
            {
                await Db.ExecuteAsync("INSERT INTO ls_cfail VALUES (1, 'a'), (2, 'b')");

                await Wait.UntilAsync(() => logs.Contains(LogLevel.Error, "The output did not acknowledge"), "the failed confirm");

                slotAtFailure = await ConfirmedLsnAsync("ls_cfail");

                await Wait.UntilAsync(() => publisher.Messages.Count >= 4, "the replay", 60);
                await Wait.UntilAsync(() => publisher.ConfirmedBatches.Sum() >= 2, "the confirm");
            }

            Assert.AreEqual(before, slotAtFailure, "the slot moved although the output did not acknowledge the batch");
            Assert.AreEqual("[1] [2] [1] [2]", Keys(publisher));
        }

        [TestMethod]
        public async Task Publisher_that_saves_its_position_should_not_get_replayed_changes_unless_deduplication_is_off()
        {
            await Db.ExecuteAsync(@"
                CREATE TABLE ls_dedup (id int PRIMARY KEY, v text);
                CREATE PUBLICATION ls_dedup_pub FOR TABLE ls_dedup;");

            // the other two slots stay at the start, so a listener on them gets every change again
            await Db.CreateSlotAsync("ls_dedup_slot");
            await Db.CreateSlotAsync("ls_dedup_replay_slot");
            await Db.CreateSlotAsync("ls_dedup_off_slot");

            // this publisher returns its last confirmed position from GetLastPublishedWalSeqAsync, as the stores do
            var publisher = new CollectingPublisherFactory();

            await using (new RunningListener(CreateBuilder("ls_dedup", publisher)))
            {
                await Db.ExecuteAsync("INSERT INTO ls_dedup VALUES (1, 'a'), (2, 'b'); INSERT INTO ls_dedup VALUES (3, 'c');");

                await Wait.UntilAsync(() => publisher.Messages.Count >= 3, "3 rows");
                await Wait.UntilAsync(() => publisher.LastConfirmed.TxFinalLsn == publisher.Messages[2].TxFinalLsn, "the confirm");
            }

            await Db.ExecuteAsync("INSERT INTO ls_dedup VALUES (4, 'd')");

            using var logs = new CollectingLoggerFactory();

            await using (new RunningListener(CreateBuilder("ls_dedup", publisher).WithPgReplicationSlot("ls_dedup_replay_slot").WithLoggerFactory(logs)))
            {
                await Wait.UntilAsync(() => publisher.Messages.Count >= 4, "the new row");
            }

            Assert.AreEqual("[1] [2] [3] [4]", Keys(publisher));
            Assert.IsTrue(logs.Contains(LogLevel.Warning, "skipped 3 already published messages"));

            await using (new RunningListener(CreateBuilder("ls_dedup", publisher).WithPgReplicationSlot("ls_dedup_off_slot").WithDeduplication(false)))
            {
                await Wait.UntilAsync(() => publisher.Messages.Count >= 8, "everything again");
            }

            Assert.AreEqual("[1] [2] [3] [4] [1] [2] [3] [4]", Keys(publisher));
        }

        [TestMethod]
        public async Task Idle_wal_message_should_move_the_slot_when_only_other_tables_change()
        {
            await CreateAsync("ls_idle");
            await Db.ExecuteAsync("CREATE TABLE ls_idle_other (id int PRIMARY KEY)");

            var publisher = new CollectingPublisherFactory();

            await using (new RunningListener(CreateBuilder("ls_idle", publisher).WithIdleWalMessageInterval(TimeSpan.FromMilliseconds(200))))
            {
                await Wait.UntilAsync(async () => await Db.ScalarAsync<bool>("SELECT active FROM pg_replication_slots WHERE slot_name = 'ls_idle_slot'"), "the listener");

                await Db.ExecuteAsync("INSERT INTO ls_idle_other SELECT generate_series(1, 1000)");

                var afterInsert = await Db.ScalarAsync<NpgsqlLogSequenceNumber>("SELECT pg_current_wal_insert_lsn()");

                await Wait.UntilAsync(async () => await ConfirmedLsnAsync("ls_idle") >= afterInsert, "the slot to move");
            }

            Assert.AreEqual(0, publisher.Messages.Count, "the keepalive message was published");
        }

        [TestMethod]
        public async Task Only_the_tables_of_the_publications_should_be_published()
        {
            await Db.ExecuteAsync(@"
                CREATE TABLE ls_pub_a (id int PRIMARY KEY);
                CREATE TABLE ls_pub_b (id int PRIMARY KEY);
                CREATE TABLE ls_pub_c (id int PRIMARY KEY);
                CREATE PUBLICATION ls_pub_pub FOR TABLE ls_pub_a;
                CREATE PUBLICATION ls_pub_pub2 FOR TABLE ls_pub_b WITH (publish = 'insert');");

            await Db.CreateSlotAsync("ls_pub_slot");

            var publisher = new CollectingPublisherFactory();

            await using (new RunningListener(CreateBuilder("ls_pub", publisher).WithPgPublications("ls_pub_pub", "ls_pub_pub2")))
            {
                await Db.ExecuteAsync(@"
                    INSERT INTO ls_pub_c VALUES (1);
                    INSERT INTO ls_pub_b VALUES (1);
                    DELETE FROM ls_pub_b;
                    TRUNCATE ls_pub_a;
                    INSERT INTO ls_pub_a VALUES (1);");

                await Wait.UntilAsync(() => publisher.Messages.Count >= 2, "2 rows");
            }

            // c is not published, the delete of b is not in its publication, a truncate is not a row change
            Assert.AreEqual("public.ls_pub_b[1] public.ls_pub_a[1]", string.Join(" ", publisher.Messages.Select(m => m.Table + m.Key)));
        }

        [TestMethod]
        public async Task Missing_slot_should_be_logged_and_tried_again()
        {
            await CreateAsync("ls_missing", createSlot: false);

            using var logs = new CollectingLoggerFactory();

            var publisher = new CollectingPublisherFactory();

            await using (new RunningListener(CreateBuilder("ls_missing", publisher).WithLoggerFactory(logs).WithReconnectDelay(TimeSpan.FromMilliseconds(500))))
            {
                await Wait.UntilAsync(() => logs.Contains(LogLevel.Error, "Replication slot ls_missing_slot does not exist"), "the error");

                await Db.CreateSlotAsync("ls_missing_slot");
                await Db.ExecuteAsync("INSERT INTO ls_missing VALUES (1, 'a')");

                await Wait.UntilAsync(() => publisher.Messages.Count >= 1, "the row after the slot was created", 60);
            }
        }

        [TestMethod]
        public async Task Reconnect_delay_should_set_the_time_between_attempts()
        {
            await CreateAsync("ls_delay", createSlot: false);

            using var logs = new CollectingLoggerFactory();

            var builder = CreateBuilder("ls_delay", new CollectingPublisherFactory())
                .WithLoggerFactory(logs)
                .WithReconnectDelay(TimeSpan.FromSeconds(2));

            await using (new RunningListener(builder))
            {
                // the slot does not exist, so every attempt fails
                await Task.Delay(TimeSpan.FromSeconds(5));
            }

            var attempts = logs.ToString().Split("Replication slot ls_delay_slot does not exist").Length - 1;

            // at 0, 2 and 4 seconds, give or take one
            Assert.IsTrue(attempts is >= 2 and <= 4, $"{attempts} attempts in 5 seconds");
            Assert.IsTrue(logs.Contains(LogLevel.Error, "Waiting for 2 seconds"));
        }

        [TestMethod]
        public async Task Stopping_should_end_the_listener_promptly()
        {
            await CreateAsync("ls_cancel");

            using var cts = new CancellationTokenSource();
            using var pgOutput2Json = CreateBuilder("ls_cancel", new CollectingPublisherFactory()).Build();

            var task = pgOutput2Json.StartAsync(cts.Token);

            await Wait.UntilAsync(async () => await Db.ScalarAsync<bool>("SELECT active FROM pg_replication_slots WHERE slot_name = 'ls_cancel_slot'"), "the listener");

            cts.Cancel();

            await task.WaitAsync(TimeSpan.FromSeconds(5));

            await Wait.UntilAsync(async () => !await Db.ScalarAsync<bool>("SELECT active FROM pg_replication_slots WHERE slot_name = 'ls_cancel_slot'"), "the slot to be released");
        }
    }
}
