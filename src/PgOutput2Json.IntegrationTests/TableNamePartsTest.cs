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
    /// The schema and the table name come separately, besides "schema.table", in which a name with a dot is quoted:
    /// "tn.s"."t.x" and tn."s.t.x" (both tn.s.t.x without the quotes) are two tables, also for the listener.
    /// </summary>
    [TestClass]
    public class TableNamePartsTest
    {
        private sealed class CollectingFactory(bool rows) : IMessagePublisherFactory
        {
            public readonly ConcurrentQueue<string> Lines = new();

            public IMessagePublisher CreateMessagePublisher(ReplicationListenerOptions listenerOptions, string slotName, ILoggerFactory? loggerFactory)
            {
                return rows ? new RowPublisher(this) : new JsonPublisher(this);
            }

            private sealed class JsonPublisher(CollectingFactory owner) : MessagePublisher
            {
                public override Task PublishAsync(JsonMessage message, CancellationToken token)
                {
                    owner.Lines.Enqueue($"{(message.IsTableSync ? "copy" : "stream")} {message.TableName} | {message.SchemaName} | {message.RelationName}");
                    return Task.CompletedTask;
                }

                public override Task ConfirmAsync(CancellationToken token) => Task.CompletedTask;

                public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
            }

            private sealed class RowPublisher(CollectingFactory owner) : MessagePublisher, IRowPublisher
            {
                public Task PublishRowAsync(ChangeRow row, CancellationToken token)
                {
                    owner.Lines.Enqueue($"{(row.ChangeType == RowChangeType.Copy ? "copy" : "stream")} {row.Table.Name} | {row.Table.SchemaName} | {row.Table.RelationName}");
                    return Task.CompletedTask;
                }

                public override Task PublishAsync(JsonMessage message, CancellationToken token) => Task.CompletedTask;

                public override Task ConfirmAsync(CancellationToken token) => Task.CompletedTask;

                public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
            }
        }

        [TestMethod]
        [DataRow(false, "tnj", false)]
        [DataRow(true, "tnr", false)]
        [DataRow(false, "tncj", true)]
        [DataRow(true, "tncr", true)]
        public async Task Messages_and_rows_should_have_the_schema_and_the_table_name_apart(bool rows, string name, bool sameJoinedName)
        {
            // with sameJoinedName the second table is "name"."s.t.x": name.s.t.x as the first one, "name.s"."t.x"
            var other = sameJoinedName ? "s.t.x" : "a.b";

            // tables of their own for each case
            await Db.ExecuteAsync($@"
                CREATE SCHEMA ""{name}.s"";
                CREATE SCHEMA {name};
                CREATE TABLE ""{name}.s"".""t.x"" (id int PRIMARY KEY);
                CREATE TABLE {name}.""{other}"" (id int PRIMARY KEY);
                INSERT INTO ""{name}.s"".""t.x"" VALUES (1);
                INSERT INTO {name}.""{other}"" VALUES (1);
                CREATE PUBLICATION {name}_pub FOR TABLE ""{name}.s"".""t.x"", {name}.""{other}"";");

            var slot = $"{name}_slot";

            await Db.CreateSlotAsync(slot);

            var factory = new CollectingFactory(rows);

            var builder = PgOutput2JsonBuilder.Create()
                .WithPgConnectionString(Containers.PgConnectionString)
                .WithPgPublications($"{name}_pub")
                .WithPgReplicationSlot(slot)
                .WithInitialDataCopy(true)
                .WithMessagePublisherFactory(factory);

            List<string> Sorted() => factory.Lines.ToList().OrderBy(l => l, System.StringComparer.Ordinal).ToList();

            var expected = new[]
            {
                $"copy \"{name}.s\".\"t.x\" | {name}.s | t.x",
                $"copy {name}.\"{other}\" | {name} | {other}",
                $"stream \"{name}.s\".\"t.x\" | {name}.s | t.x",
                $"stream {name}.\"{other}\" | {name} | {other}",
            };

            await using (new RunningListener(builder))
            {
                try
                {
                    await Wait.UntilAsync(() => factory.Lines.Count >= 2, "the copied rows");
                }
                catch (System.TimeoutException e)
                {
                    Assert.Fail($"{e.Message}: {string.Join(" / ", Sorted())}");
                }

                await Db.ExecuteAsync($@"INSERT INTO ""{name}.s"".""t.x"" VALUES (2); INSERT INTO {name}.""{other}"" VALUES (2);");

                try
                {
                    await Wait.UntilAsync(() => factory.Lines.Count == 4, "the streamed rows");
                }
                catch (System.TimeoutException)
                {
                }
            }

            CollectionAssert.AreEqual(expected, Sorted(), string.Join("\n", Sorted()));
        }
    }
}
