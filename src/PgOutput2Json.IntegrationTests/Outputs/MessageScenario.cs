using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace PgOutput2Json.IntegrationTests.Outputs
{
    /// <summary>
    /// The same changes for every output that forwards messages: two inserts in one transaction,
    /// an update and a delete. The output must receive the four messages, in this order.
    /// </summary>
    internal static class MessageScenario
    {
        public const string Expected = "I[1] I[2] U[1] D[2]";

        /// <summary>
        /// Runs the scenario. <paramref name="readAsync"/> returns the JSON of the messages at the output;
        /// it is polled until there are four. The table is public.{name}, with columns id, tenant and v.
        /// </summary>
        public static async Task<List<string>> RunAsync(string name,
                                                        Func<PgOutput2JsonBuilder, PgOutput2JsonBuilder> useOutput,
                                                        Func<Task<List<string>>> readAsync,
                                                        bool expectErrors = false)
        {
            await Db.ExecuteAsync($@"
                CREATE TABLE {name} (id int PRIMARY KEY, tenant int, v text);
                CREATE PUBLICATION {name}_pub FOR TABLE {name};");

            await Db.CreateSlotAsync($"{name}_slot");

            using var logs = new CollectingLoggerFactory();

            var builder = useOutput(PgOutput2JsonBuilder.Create()
                .WithLoggerFactory(logs)
                .WithPgConnectionString(Containers.PgConnectionString)
                .WithPgPublications($"{name}_pub")
                .WithPgReplicationSlot($"{name}_slot")
                // the slot moves past the last change only with the next message, which on an idle database is this one
                .WithIdleWalMessageInterval(TimeSpan.FromMilliseconds(200)));

            var messages = new List<string>();

            await using (new RunningListener(builder))
            {
                await Db.ExecuteAsync($@"
                    INSERT INTO {name} VALUES (1, 7, 'a'), (2, 8, 'b');
                    UPDATE {name} SET v = 'c' WHERE id = 1;
                    DELETE FROM {name} WHERE id = 2;");

                try
                {
                    await Wait.UntilAsync(async () => (messages = await readAsync()).Count >= 4, "4 messages at the output", 60);
                }
                catch (TimeoutException)
                {
                    Assert.Fail($"Only {messages.Count} messages at the output: {Describe(messages)}. Logged: {logs}");
                }

                // the batch is acknowledged by the output, so PostgreSQL gets the confirmation
                var afterChanges = await Db.ScalarAsync<NpgsqlTypes.NpgsqlLogSequenceNumber>("SELECT pg_current_wal_lsn()");

                await Wait.UntilAsync(
                    async () => await Db.ScalarAsync<NpgsqlTypes.NpgsqlLogSequenceNumber>($"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '{name}_slot'") >= afterChanges,
                    "the slot to move");
            }

            Assert.AreEqual(Expected, Describe(messages));
            Assert.AreEqual(expectErrors, logs.Contains(LogLevel.Error, ""), $"logged: {logs}");

            return messages;
        }

        /// <summary>
        /// The change type and the id of each message, e.g. "I[1] U[1]".
        /// </summary>
        public static string Describe(IEnumerable<string> messages)
        {
            return string.Join(" ", messages.Select(message =>
            {
                using var json = JsonDocument.Parse(message);

                var root = json.RootElement;
                var row = root.TryGetProperty("r", out var r) ? r : root.GetProperty("k");

                return $"{root.GetProperty("c").GetString()}[{row.GetProperty("id").GetInt32()}]";
            }));
        }
    }
}
