using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Npgsql;

namespace PgOutput2Json.IntegrationTests
{
    [TestClass]
    public class TableSyncTest
    {
        private static PgOutput2JsonBuilder CreateBuilder(string name, CollectingPublisherFactory publisher, bool copyData = true)
        {
            return PgOutput2JsonBuilder.Create()
                .WithPgConnectionString(Containers.PgConnectionString)
                .WithPgPublications($"{name}_pub")
                .WithPgReplicationSlot($"{name}_slot")
                .WithInitialDataCopy(copyData)
                .WithJsonOptions(options => options.WriteTableNames = true)
                .WithMessagePublisherFactory(publisher);
        }

        private static string Describe(IEnumerable<PublishedMessage> messages)
        {
            return string.Join(" ", messages.Select(m => $"{ChangeType(m)}{m.Key}"));
        }

        private static string ChangeType(PublishedMessage message)
        {
            using var json = JsonDocument.Parse(message.Json);

            return json.RootElement.GetProperty("c").GetString()!;
        }

        /// <summary>
        /// Applies the messages of a table, in the order they were published, the way a consumer would.
        /// </summary>
        private static Dictionary<int, string> Apply(IEnumerable<PublishedMessage> messages, string table)
        {
            var rows = new Dictionary<int, string>();

            foreach (var message in messages.Where(m => m.Table == table))
            {
                using var json = JsonDocument.Parse(message.Json);

                var root = json.RootElement;

                if (root.GetProperty("c").GetString() == "D")
                {
                    rows.Remove(root.GetProperty("k").GetProperty("id").GetInt32());
                }
                else
                {
                    rows[root.GetProperty("r").GetProperty("id").GetInt32()] = root.GetProperty("r").GetProperty("v").GetString()!;
                }
            }

            return rows;
        }

        private static async Task<Dictionary<int, string>> ReadTableAsync(string table)
        {
            var rows = new Dictionary<int, string>();

            await using var connection = new NpgsqlConnection(Containers.PgConnectionString);
            await connection.OpenAsync();

            await using var command = new NpgsqlCommand($"SELECT id, v FROM {table}", connection);
            await using var reader = await command.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                rows[reader.GetInt32(0)] = reader.GetString(1);
            }

            return rows;
        }

        private static void AssertSameRows(Dictionary<int, string> expected, Dictionary<int, string> actual, string table)
        {
            var differences = expected.Keys.Union(actual.Keys)
                .Where(id => !expected.TryGetValue(id, out var e) || !actual.TryGetValue(id, out var a) || e != a)
                .OrderBy(id => id)
                .Take(10)
                .Select(id => $"{id}: table={(expected.TryGetValue(id, out var e) ? e : "-")} output={(actual.TryGetValue(id, out var a) ? a : "-")}")
                .ToList();

            Assert.AreEqual(0, differences.Count, $"{table} differs: {string.Join(", ", differences)}");
        }

        /// <summary>
        /// Inserts, updates and deletes rows of the tables, each statement its own transaction, until cancelled.
        /// </summary>
        private static async Task WriteAsync(string[] tables, int firstNewId, CancellationToken token)
        {
            var random = new Random(1);
            var nextId = firstNewId;

            await using var connection = new NpgsqlConnection(Containers.PgConnectionString);
            await connection.OpenAsync();

            for (var i = 0; !token.IsCancellationRequested; i++)
            {
                var table = tables[i % tables.Length];
                var id = random.Next(1, nextId);

                var sql = random.Next(4) switch
                {
                    0 => $"INSERT INTO {table} VALUES ({nextId++}, 'new')",
                    1 => $"DELETE FROM {table} WHERE id = {id}",
                    _ => $"UPDATE {table} SET v = 'u{i}' WHERE id = {id}",
                };

                await using var command = new NpgsqlCommand(sql, connection);
                await command.ExecuteNonQueryAsync();
            }
        }

        [TestMethod]
        public async Task Writes_during_the_sync_should_end_in_the_same_rows_at_the_output()
        {
            var tables = new[] { "ts_wr_a", "ts_wr_b", "ts_wr_c", "ts_wr_d" };

            // d is not copied (registered while the copy is off), so the main stream has work throughout
            await Db.ExecuteAsync(string.Concat(tables.Select(t => $@"
                CREATE TABLE {t} (id int PRIMARY KEY, v text);
                INSERT INTO {t} SELECT i, 'copy' FROM generate_series(1, 5000) i;")) + @"
                CREATE PUBLICATION ts_wr_pub FOR TABLE ts_wr_d;");

            await Db.CreateSlotAsync("ts_wr_slot");

            using var logs = new CollectingLoggerFactory();

            var publisher = new CollectingPublisherFactory();

            await using (new RunningListener(CreateBuilder("ts_wr", publisher, copyData: false)))
            {
                await Wait.UntilAsync(async () => await Db.TableStatesAsync("ts_wr_slot") == "public.ts_wr_d:ready", "the table state");
            }

            await Db.ExecuteAsync("ALTER PUBLICATION ts_wr_pub ADD TABLE ts_wr_a, ts_wr_b, ts_wr_c");

            publisher.SyncConfirmDelay = TimeSpan.FromMilliseconds(20);

            using var stopWriting = new CancellationTokenSource();

            var writer = Task.Run(() => WriteAsync(tables, 5001, stopWriting.Token));

            await using (new RunningListener(CreateBuilder("ts_wr", publisher).WithLoggerFactory(logs)))
            {
                await Wait.UntilAsync(
                    async () => await Db.TableStatesAsync("ts_wr_slot") == "public.ts_wr_a:ready,public.ts_wr_b:ready,public.ts_wr_c:ready,public.ts_wr_d:ready",
                    "all tables ready", 120);

                await Wait.UntilAsync(async () => await CountSyncSlotsAsync("ts_wr") == 0, "the sync slots dropped");

                // keep writing for a while after the handover
                await Task.Delay(500);

                stopWriting.Cancel();
                await writer;

                await Db.ExecuteAsync("INSERT INTO ts_wr_d VALUES (0, 'last')");

                await Wait.UntilAsync(() => publisher.Messages.Any(m => m.Table == "public.ts_wr_d" && m.Key == "[0]"), "the last row");
            }

            var messages = publisher.Messages;

            foreach (var table in tables.Take(3))
            {
                AssertSameRows(await ReadTableAsync(table), Apply(messages, $"public.{table}"), table);
            }

            // nothing was restarted, so no change may be published twice
            var changes = messages.Where(m => ChangeType(m) != "C").ToList();

            Assert.AreEqual(changes.Count, changes.Select(m => (m.TxFinalLsn, m.MessageNo)).Distinct().Count(), "a change was published twice");

            // the test is only worth something if the workers had to catch up and the main stream took over
            foreach (var table in tables.Take(3))
            {
                Assert.IsTrue(messages.Count(m => m.Table == $"public.{table}" && ChangeType(m) == "C") > 4000, $"{table} was not copied");
                Assert.IsTrue(changes.Any(m => m.Table == $"public.{table}" && m.IsTableSync), $"no change of {table} came from the sync worker");
                Assert.IsTrue(changes.Any(m => m.Table == $"public.{table}" && !m.IsTableSync), $"no change of {table} came from the main stream");
            }

            Assert.IsFalse(messages.Any(m => m.Table == "public.ts_wr_d" && m.IsTableSync), "the ready table was synced");

            Assert.IsFalse(logs.Contains(LogLevel.Error, ""), $"errors were logged: {logs}");
            Assert.IsFalse(logs.Contains(LogLevel.Warning, "Could not"), $"warnings were logged: {logs}");
        }

        [TestMethod]
        public async Task Table_published_while_running_should_be_synced_without_a_restart()
        {
            await Db.ExecuteAsync(@"
                CREATE TABLE ts_new_a (id int PRIMARY KEY, v text);
                CREATE TABLE ts_new_b (id int PRIMARY KEY, v text);
                INSERT INTO ts_new_a VALUES (1, 'a');
                INSERT INTO ts_new_b VALUES (10, 'b'), (11, 'b');
                CREATE PUBLICATION ts_new_pub FOR TABLE ts_new_a;");

            await Db.CreateSlotAsync("ts_new_slot");

            var publisher = new CollectingPublisherFactory();

            await using (new RunningListener(CreateBuilder("ts_new", publisher)))
            {
                await Wait.UntilAsync(async () => await Db.TableStatesAsync("ts_new_slot") == "public.ts_new_a:ready", "the first table ready");

                await Db.ExecuteAsync("ALTER PUBLICATION ts_new_pub ADD TABLE ts_new_b");

                // the listener learns about the table from its first change
                await Db.ExecuteAsync("INSERT INTO ts_new_b VALUES (12, 'b')");

                await Wait.UntilAsync(async () => await Db.TableStatesAsync("ts_new_slot") == "public.ts_new_a:ready,public.ts_new_b:ready", "the new table ready");

                await Db.ExecuteAsync("UPDATE ts_new_b SET v = 'c' WHERE id = 10");

                await Wait.UntilAsync(() => publisher.Messages.Count >= 5, "the update");
            }

            Assert.AreEqual("C[1] C[10] C[11] C[12] U[10]", Describe(publisher.Messages));
        }

        [TestMethod]
        public async Task Copy_should_apply_the_row_filter_and_the_column_list()
        {
            await Db.ExecuteAsync(@"
                CREATE TABLE ts_flt (id int PRIMARY KEY, v text, secret text);
                INSERT INTO ts_flt VALUES (1, 'a', 'x'), (2, '', 'x'), (3, NULL, 'x');
                CREATE PUBLICATION ts_flt_pub FOR TABLE ts_flt (id, v) WHERE (id > 1);");

            await Db.CreateSlotAsync("ts_flt_slot");

            var publisher = new CollectingPublisherFactory();

            await using (new RunningListener(CreateBuilder("ts_flt", publisher).WithJsonOptions(options => options.WriteNulls = true)))
            {
                await Wait.UntilAsync(async () => await Db.TableStatesAsync("ts_flt_slot") == "public.ts_flt:ready", "the table ready");

                await Db.ExecuteAsync("INSERT INTO ts_flt VALUES (4, 'd', 'x')");

                await Wait.UntilAsync(() => publisher.Messages.Count >= 3, "the streamed row");
            }

            var rows = publisher.Messages.Select(m =>
            {
                using var json = JsonDocument.Parse(m.Json);

                return json.RootElement.GetProperty("r").GetRawText();
            }).ToArray();

            // an empty string in the last column, a null, and the same columns from the stream
            CollectionAssert.AreEqual(new[] { "{\"id\":2,\"v\":\"\"}", "{\"id\":3,\"v\":null}", "{\"id\":4,\"v\":\"d\"}" }, rows);
        }

        [TestMethod]
        public async Task Temporary_slot_should_copy_and_then_stream()
        {
            await Db.ExecuteAsync(@"
                CREATE TABLE ts_tmp (id int PRIMARY KEY, v text);
                INSERT INTO ts_tmp VALUES (1, 'a'), (2, 'b');
                CREATE PUBLICATION ts_tmp_pub FOR TABLE ts_tmp;");

            var publisher = new CollectingPublisherFactory();

            var builder = CreateBuilder("ts_tmp", publisher).WithPgReplicationSlot("ts_tmp_slot", useTemporarySlot: true);

            await using (new RunningListener(builder))
            {
                await Wait.UntilAsync(() => publisher.Messages.Count >= 2, "the copied rows");

                // published by the sync worker or by the main stream, depending on where the handover is
                await Db.ExecuteAsync("INSERT INTO ts_tmp VALUES (3, 'c')");

                await Wait.UntilAsync(() => publisher.Messages.Count >= 3, "the streamed row");

                await Db.ExecuteAsync("INSERT INTO ts_tmp VALUES (4, 'd')");

                await Wait.UntilAsync(() => publisher.Messages.Count >= 4, "the second streamed row");
            }

            Assert.AreEqual("C[1] C[2] I[3] I[4]", Describe(publisher.Messages));
            Assert.AreEqual("", await Db.TableStatesAsync("ts_tmp_slot"));
        }

        /// <summary>
        /// Leaves a table the way a listener that was stopped in the given state leaves it: with a sync slot,
        /// and with a row inserted after the slot was created.
        /// </summary>
        private static async Task<CollectingPublisherFactory> PrepareStoppedSyncAsync(string name, string state)
        {
            await Db.ExecuteAsync($@"
                CREATE TABLE {name} (id int PRIMARY KEY, v text);
                INSERT INTO {name} VALUES (1, 'a'), (2, 'b');
                CREATE PUBLICATION {name}_pub FOR TABLE {name};");

            await Db.CreateSlotAsync($"{name}_slot");

            var publisher = new CollectingPublisherFactory();

            await using (new RunningListener(CreateBuilder(name, publisher, copyData: false)))
            {
                await Wait.UntilAsync(async () => await Db.TableStatesAsync($"{name}_slot") == $"public.{name}:ready", "the table state");
            }

            await Db.ExecuteAsync($@"
                UPDATE pgoutput2json.table_sync_state
                SET state = '{state}', lsn = (SELECT lsn FROM pg_create_logical_replication_slot('{name}_slot_sync_' || '{name}'::regclass::oid, 'pgoutput'))
                WHERE slot_name = '{name}_slot';

                INSERT INTO {name} VALUES (3, 'c');");

            return publisher;
        }

        private static Task<long> CountSyncSlotsAsync(string name)
        {
            return Db.ScalarAsync<long>($"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{name}_slot_sync_%'");
        }

        [TestMethod]
        public async Task Sync_stopped_after_the_copy_should_resume_from_its_slot()
        {
            var publisher = await PrepareStoppedSyncAsync("ts_copied", "copied");

            await using (new RunningListener(CreateBuilder("ts_copied", publisher)))
            {
                await Wait.UntilAsync(async () => await Db.TableStatesAsync("ts_copied_slot") == "public.ts_copied:ready", "the table ready");
                await Wait.UntilAsync(async () => await CountSyncSlotsAsync("ts_copied") == 0, "the sync slot dropped");

                await Db.ExecuteAsync("INSERT INTO ts_copied VALUES (4, 'd')");

                await Wait.UntilAsync(() => publisher.Messages.Count >= 2, "the streamed row");
            }

            // no copy: only what happened after the sync slot was created, once
            Assert.AreEqual("I[3] I[4]", Describe(publisher.Messages));
            Assert.IsTrue(publisher.Messages[0].IsTableSync);
            Assert.IsFalse(publisher.Messages[1].IsTableSync);
        }

        [TestMethod]
        public async Task Sync_stopped_during_the_copy_should_copy_again_from_zero()
        {
            var publisher = await PrepareStoppedSyncAsync("ts_copying", "copying");

            await using (new RunningListener(CreateBuilder("ts_copying", publisher)))
            {
                await Wait.UntilAsync(async () => await Db.TableStatesAsync("ts_copying_slot") == "public.ts_copying:ready", "the table ready");
                await Wait.UntilAsync(async () => await CountSyncSlotsAsync("ts_copying") == 0, "the sync slot dropped");
            }

            Assert.AreEqual("C[1] C[2] C[3]", Describe(publisher.Messages));
        }

        [TestMethod]
        public async Task Listener_stopped_during_the_copy_should_copy_again_and_end_in_the_same_rows()
        {
            await Db.ExecuteAsync(@"
                CREATE TABLE ts_stop (id int PRIMARY KEY, v text);
                INSERT INTO ts_stop SELECT i, 'copy' FROM generate_series(1, 2000) i;
                CREATE PUBLICATION ts_stop_pub FOR TABLE ts_stop;");

            await Db.CreateSlotAsync("ts_stop_slot");

            var publisher = new CollectingPublisherFactory { SyncConfirmDelay = TimeSpan.FromMilliseconds(100) };

            await using (new RunningListener(CreateBuilder("ts_stop", publisher)))
            {
                await Wait.UntilAsync(() => publisher.Messages.Count >= 300, "a part of the copy");
            }

            Assert.AreEqual("public.ts_stop:copying", await Db.TableStatesAsync("ts_stop_slot"));
            Assert.IsTrue(publisher.Messages.Count < 2000, "the copy was not interrupted");

            // changes between the two attempts: the second copy must bring them, and remove nothing it should not
            await Db.ExecuteAsync(@"
                UPDATE ts_stop SET v = 'changed' WHERE id <= 10;
                DELETE FROM ts_stop WHERE id > 1990;
                INSERT INTO ts_stop VALUES (3000, 'new');");

            publisher.SyncConfirmDelay = TimeSpan.Zero;

            await using (new RunningListener(CreateBuilder("ts_stop", publisher)))
            {
                await Wait.UntilAsync(async () => await Db.TableStatesAsync("ts_stop_slot") == "public.ts_stop:ready", "the table ready");
                await Wait.UntilAsync(async () => await CountSyncSlotsAsync("ts_stop") == 0, "the sync slot dropped");
            }

            // the rows copied by the first attempt and deleted since stay at the output: a copy cannot delete
            var expected = await ReadTableAsync("ts_stop");
            var actual = Apply(publisher.Messages, "public.ts_stop");

            foreach (var id in actual.Keys.Where(id => id > 1990 && id <= 2000).ToList())
            {
                actual.Remove(id);
            }

            AssertSameRows(expected, actual, "ts_stop");
        }

        [TestMethod]
        public async Task Copy_turned_off_should_stream_the_unsynced_table_and_drop_its_sync_slot()
        {
            var publisher = await PrepareStoppedSyncAsync("ts_off", "copied");

            await using (new RunningListener(CreateBuilder("ts_off", publisher, copyData: false)))
            {
                await Db.ExecuteAsync("INSERT INTO ts_off VALUES (4, 'd')");

                await Wait.UntilAsync(() => publisher.Messages.Count >= 2, "the streamed rows");
            }

            Assert.AreEqual("I[3] I[4]", Describe(publisher.Messages));
            Assert.IsFalse(publisher.Messages[0].IsTableSync);
            Assert.AreEqual(0L, await CountSyncSlotsAsync("ts_off"));

            // still to be copied if the copy is turned on again
            Assert.AreEqual("public.ts_off:init", await Db.TableStatesAsync("ts_off_slot"));
        }

        [TestMethod]
        public async Task Second_listener_on_a_slot_in_use_should_not_start_a_sync()
        {
            await Db.ExecuteAsync(@"
                CREATE TABLE ts_busy_a (id int PRIMARY KEY, v text);
                CREATE TABLE ts_busy_b (id int PRIMARY KEY, v text);
                INSERT INTO ts_busy_b VALUES (1, 'b');
                CREATE PUBLICATION ts_busy_pub FOR TABLE ts_busy_a;");

            await Db.CreateSlotAsync("ts_busy_slot");

            using var logs = new CollectingLoggerFactory();

            var first = new CollectingPublisherFactory();
            var second = new CollectingPublisherFactory();

            await using (new RunningListener(CreateBuilder("ts_busy", first)))
            {
                await Wait.UntilAsync(async () => await Db.TableStatesAsync("ts_busy_slot") == "public.ts_busy_a:ready", "the table ready");
                await Wait.UntilAsync(async () => await Db.ScalarAsync<bool>("SELECT active FROM pg_replication_slots WHERE slot_name = 'ts_busy_slot'"), "the slot in use");

                // the first listener does not know the table yet, and the second one must leave it alone
                await Db.ExecuteAsync("ALTER PUBLICATION ts_busy_pub ADD TABLE ts_busy_b");

                await using (new RunningListener(CreateBuilder("ts_busy", second).WithLoggerFactory(logs)))
                {
                    await Wait.UntilAsync(() => logs.Contains(LogLevel.Warning, "Slot taken"), "the second listener to give up");
                }
            }

            Assert.AreEqual(0, second.Messages.Count);
            Assert.AreEqual("public.ts_busy_a:ready", await Db.TableStatesAsync("ts_busy_slot"));
        }
    }
}
