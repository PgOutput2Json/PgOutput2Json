using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace PgOutput2Json.IntegrationTests
{
    /// <summary>
    /// A publisher that implements <see cref="IRowPublisher"/> gets the rows instead of the JSON.
    /// </summary>
    [TestClass]
    public class RowPublisherTest
    {
        /// <summary>
        /// Writes down every row as "change table new=[...] old=[...]", and every new table instance as "table: column ...".
        /// </summary>
        private sealed class CollectingRowPublisherFactory : IMessagePublisherFactory
        {
            private readonly ConcurrentQueue<string> _lines = new();

            public List<string> Lines => _lines.ToList();

            public IMessagePublisher CreateMessagePublisher(ReplicationListenerOptions listenerOptions, string slotName, ILoggerFactory? loggerFactory)
            {
                return new Publisher(this);
            }

            private sealed class Publisher(CollectingRowPublisherFactory owner) : MessagePublisher, IRowPublisher
            {
                private readonly HashSet<RowTable> _tables = [];

                public Task PublishRowAsync(ChangeRow row, CancellationToken token)
                {
                    if (_tables.Add(row.Table))
                    {
                        owner._lines.Enqueue($"{row.Table.Name}: " + string.Join(" ", row.Table.Columns.Select(c =>
                            $"{c.Name}:{c.TypeId}:{c.TypeModifier}{(c.TypeName != null ? ":" + c.TypeName : "")}{(c.IsKey ? ":key" : "")}{(c.IsPartitionKey ? ":partition" : "")}")));
                    }

                    var position = row.ChangeType == RowChangeType.Copy
                        ? (row.TxFinalLsn == 0 && row.MessageNo == 0 && row.CommitTimestamp == default ? "" : " position!")
                        : (row.TxFinalLsn > 0 && row.MessageNo > 0 && row.CommitTimestamp != default && row.ServerClock != default ? "" : " no position!");

                    owner._lines.Enqueue($"{row.ChangeType} {row.Table.Name}"
                        + (row.NewValues != null ? $" new={Describe(row.NewValues)}" : "")
                        + (row.OldValues != null ? $" old={Describe(row.OldValues)}" : "")
                        + (row.IsTableSync ? " sync" : "")
                        + position);

                    return Task.CompletedTask;
                }

                private static string Describe(IReadOnlyList<RowValue> values)
                {
                    return "[" + string.Join(",", values.Select(v => v.Kind switch
                    {
                        RowValueKind.Null => "NULL",
                        RowValueKind.Unchanged => "~",
                        _ => v.Text!.Length > 20 ? $"{v.Text[0]}*{v.Text.Length}" : v.Text,
                    })) + "]";
                }

                public override Task PublishAsync(JsonMessage jsonMessage, CancellationToken token)
                {
                    owner._lines.Enqueue($"json {jsonMessage.Json}");

                    return Task.CompletedTask;
                }

                public override Task ConfirmAsync(CancellationToken token) => Task.CompletedTask;

                public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
            }
        }

        private static void AssertLines(string[] expected, IEnumerable<string> actual)
        {
            Assert.AreEqual(string.Join("\n", expected), string.Join("\n", actual));
        }

        [TestMethod]
        public async Task Row_publisher_should_get_the_rows_of_the_stream()
        {
            await Db.ExecuteAsync(@"
                CREATE TYPE rp_mood AS ENUM ('ok', 'bad');
                CREATE TABLE rp_stream (id int PRIMARY KEY, v varchar(10), big text, m rp_mood, skipped int);
                ALTER TABLE rp_stream ALTER COLUMN big SET STORAGE EXTERNAL;
                CREATE TABLE rp_full (id int PRIMARY KEY, v text);
                ALTER TABLE rp_full REPLICA IDENTITY FULL;
                CREATE PUBLICATION rp_stream_pub FOR TABLE rp_stream, rp_full;");

            await Db.CreateSlotAsync("rp_stream_slot");

            var publisher = new CollectingRowPublisherFactory();

            var builder = PgOutput2JsonBuilder.Create()
                .WithPgConnectionString(Containers.PgConnectionString)
                .WithPgPublications("rp_stream_pub")
                .WithPgReplicationSlot("rp_stream_slot")
                .WithPgColumns("public.rp_stream", "id", "v", "big", "m")
                .WithPartitionKeyFields("public.rp_stream", "v")
                .WithMessagePublisherFactory(publisher);

            await using (new RunningListener(builder))
            {
                await Db.ExecuteAsync(@"
                    INSERT INTO rp_stream VALUES (1, 'a', repeat('x', 3000), 'ok', 7), (2, NULL, NULL, NULL, NULL);
                    UPDATE rp_stream SET v = 'b' WHERE id = 1;
                    UPDATE rp_stream SET id = 3 WHERE id = 2;
                    DELETE FROM rp_stream WHERE id = 3;
                    INSERT INTO rp_full VALUES (1, 'a');
                    UPDATE rp_full SET v = 'b';
                    DELETE FROM rp_full;
                    SELECT pg_logical_emit_message(true, 'rp', 'hello');
                    ALTER TABLE rp_stream ADD COLUMN extra int;
                    INSERT INTO rp_stream VALUES (4, 'c', NULL, 'bad', NULL, 5);");

                await Wait.UntilAsync(() => publisher.Lines.Count >= 13, "13 lines");
            }

            var lines = publisher.Lines;

            var enumOid = await Db.ScalarAsync<uint>("SELECT 'rp_mood'::regtype::oid");

            // the logical decoding message has no row, so it comes as JSON
            Assert.AreEqual(1, lines.Count(l => l.StartsWith("json {\"c\":\"M\"")), string.Join("\n", lines));

            lines.RemoveAll(l => l.StartsWith("json "));

            AssertLines(
            [
                // varchar(10) has the modifier 14 (the length + 4), the enum its name; the column left out is not there
                $"public.rp_stream: id:23:-1:key v:1043:14:partition big:25:-1 m:{enumOid}:-1:rp_mood",
                "Insert public.rp_stream new=[1,a,x*3000,ok]",
                "Insert public.rp_stream new=[2,NULL,NULL,NULL]",
                // the big value is not sent: it did not change
                "Update public.rp_stream new=[1,b,~,ok]",
                // the key changed: the old key, with the other columns null
                "Update public.rp_stream new=[3,NULL,NULL,NULL] old=[2,NULL,NULL,NULL]",
                "Delete public.rp_stream old=[3,NULL,NULL,NULL]",
                "public.rp_full: id:23:-1:key v:25:-1:key",
                "Insert public.rp_full new=[1,a]",
                // REPLICA IDENTITY FULL: the whole old row, and every column is part of the key
                "Update public.rp_full new=[1,b] old=[1,a]",
                "Delete public.rp_full old=[1,b]",
                // a new instance after the columns changed
                $"public.rp_stream: id:23:-1:key v:1043:14:partition big:25:-1 m:{enumOid}:-1:rp_mood",
                "Insert public.rp_stream new=[4,c,NULL,bad]",
            ], lines);
        }

        [TestMethod]
        public async Task Row_publisher_should_get_the_copied_rows_and_the_changes_of_the_sync()
        {
            await Db.ExecuteAsync(@"
                CREATE TABLE rp_copy (id int PRIMARY KEY, v text, skipped int);
                INSERT INTO rp_copy VALUES (1, 'a', 1), (2, NULL, 2), (3, E'tab\there', 3);
                CREATE PUBLICATION rp_copy_pub FOR TABLE rp_copy;");

            await Db.CreateSlotAsync("rp_copy_slot");

            var publisher = new CollectingRowPublisherFactory();

            var builder = PgOutput2JsonBuilder.Create()
                .WithPgConnectionString(Containers.PgConnectionString)
                .WithPgPublications("rp_copy_pub")
                .WithPgReplicationSlot("rp_copy_slot")
                .WithPgColumns("public.rp_copy", "id", "v")
                .WithInitialDataCopy(true)
                .WithMessagePublisherFactory(publisher);

            await using (new RunningListener(builder))
            {
                await Wait.UntilAsync(async () => await Db.TableStatesAsync("rp_copy_slot") == "public.rp_copy:ready", "the table ready");

                await Db.ExecuteAsync("INSERT INTO rp_copy VALUES (4, 'd', 4)");

                await Wait.UntilAsync(() => publisher.Lines.Count(l => !l.Contains(':')) >= 4, "4 rows");
            }

            var lines = publisher.Lines;

            AssertLines(
            [
                "public.rp_copy: id:23:-1:key v:25:-1",
                "Copy public.rp_copy new=[1,a] sync",
                "Copy public.rp_copy new=[2,NULL] sync",
                "Copy public.rp_copy new=[3,tab\there] sync",
            ], lines.Take(4));

            // the table was handed over: the main stream, with a publisher of its own, has the change
            AssertLines(
            [
                "public.rp_copy: id:23:-1:key v:25:-1",
                "Insert public.rp_copy new=[4,d]",
            ], lines.Skip(4));
        }
    }
}
