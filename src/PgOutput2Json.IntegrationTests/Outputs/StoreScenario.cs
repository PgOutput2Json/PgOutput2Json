using System;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace PgOutput2Json.IntegrationTests.Outputs
{
    /// <summary>
    /// The same changes for every output that keeps a copy of the tables (SQLite, MongoDB, DynamoDB):
    /// an initial copy, then inserts, updates, deletes, a key change, nulls, arrays, binary data and an
    /// unchanged large value. The second table has its key in the last column, the third a key of two columns.
    /// </summary>
    internal static class StoreScenario
    {
        /// <summary>
        /// Runs the scenario. <paramref name="readAsync"/> returns the rows of a table in the store, in a form
        /// the test compares with what it expects; it is polled until it returns <paramref name="expected"/>.
        /// </summary>
        public static async Task RunAsync(string name,
                                          Func<PgOutput2JsonBuilder, PgOutput2JsonBuilder> useStore,
                                          Func<string, Task<string>> readAsync,
                                          string expectedAfterCopy,
                                          string expected,
                                          string expectedKeyLast,
                                          string expectedCompositeKey)
        {
            await Db.ExecuteAsync($@"
                CREATE TABLE {name} (id int PRIMARY KEY, v text, n numeric(10,2), b bool, tags text[], d bytea, big text);
                ALTER TABLE {name} ALTER COLUMN big SET STORAGE EXTERNAL;
                CREATE TABLE {name}_kl (v text, id int PRIMARY KEY);
                CREATE TABLE {name}_ck (id int, v text, code text, PRIMARY KEY (id, code));
                INSERT INTO {name} VALUES (1, 'one', 1.50, true, ARRAY['a', 'b c'], '\x0102', repeat('x', 3000)), (2, 'two', NULL, false, NULL, NULL, NULL);
                INSERT INTO {name}_kl VALUES ('copied', 1);
                INSERT INTO {name}_ck VALUES (1, 'copied', 'x'), (2, 'copied', 'y');
                CREATE PUBLICATION {name}_pub FOR TABLE {name}, {name}_kl, {name}_ck;");

            await Db.CreateSlotAsync($"{name}_slot");

            using var logs = new CollectingLoggerFactory();

            PgOutput2JsonBuilder CreateBuilder() => useStore(PgOutput2JsonBuilder.Create()
                .WithLoggerFactory(logs)
                .WithPgConnectionString(Containers.PgConnectionString)
                .WithPgPublications($"{name}_pub")
                .WithPgReplicationSlot($"{name}_slot")
                .WithInitialDataCopy(true));

            async Task WaitForAsync(string table, string expectedRows, string what)
            {
                var actual = "";

                try
                {
                    await Wait.UntilAsync(async () => (actual = await readAsync(table)) == expectedRows, what);
                }
                catch (TimeoutException)
                {
                    Assert.AreEqual(expectedRows, actual, $"{what}. Logged: {logs}");
                }
            }

            await using (new RunningListener(CreateBuilder()))
            {
                await Wait.UntilAsync(async () => await Db.TableStatesAsync($"{name}_slot") == $"public.{name}:ready,public.{name}_ck:ready,public.{name}_kl:ready", "the tables ready");

                await WaitForAsync(name, expectedAfterCopy, "the copied rows");

                await Db.ExecuteAsync($@"
                    INSERT INTO {name} VALUES (3, 'three', 3, NULL, '{{}}', '\xff', NULL);
                    UPDATE {name} SET v = 'uno' WHERE id = 1;                      -- big is not sent
                    UPDATE {name} SET v = NULL, n = 2.25, tags = ARRAY['x'] WHERE id = 2;
                    UPDATE {name} SET id = 30 WHERE id = 3;                        -- key change
                    INSERT INTO {name} VALUES (4, 'four', 4, true, NULL, NULL, NULL);
                    DELETE FROM {name} WHERE id = 4;

                    INSERT INTO {name}_kl VALUES ('new', 2), ('gone', 3);
                    UPDATE {name}_kl SET v = 'changed' WHERE id = 1;
                    DELETE FROM {name}_kl WHERE id = 3;

                    INSERT INTO {name}_ck VALUES (3, 'new', 'z');
                    UPDATE {name}_ck SET v = 'changed' WHERE id = 1;
                    UPDATE {name}_ck SET code = 'zz' WHERE id = 3;
                    DELETE FROM {name}_ck WHERE id = 2;");

                await WaitForAsync(name, expected, "the changes");
                await WaitForAsync($"{name}_kl", expectedKeyLast, "the changes of the table with the key last");
                await WaitForAsync($"{name}_ck", expectedCompositeKey, "the changes of the table with the key of two columns");
            }

            // a restart replays at most the unconfirmed tail, and the store ends in the same rows
            await using (new RunningListener(CreateBuilder()))
            {
                await Db.ExecuteAsync($"UPDATE {name}_kl SET v = 'after restart' WHERE id = 2");

                await WaitForAsync($"{name}_kl", expectedKeyLast.Replace("new", "after restart"), "the change after the restart");
                await WaitForAsync(name, expected, "the rows after the restart");
            }

            Assert.IsFalse(logs.Contains(LogLevel.Error, ""), $"errors were logged: {logs}");
        }
    }
}
