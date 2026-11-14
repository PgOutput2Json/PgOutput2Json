using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NpgsqlTypes;

namespace PgOutput2Json.IntegrationTests.Outputs
{
    /// <summary>
    /// What every output must do when it cannot take a change: report it, so the listener does not confirm
    /// the change to PostgreSQL, and deliver it once the output works again.
    /// </summary>
    internal static class FailureScenario
    {
        /// <summary>
        /// Runs the scenario. The output is broken by <paramref name="breakAsync"/>, which returns the action
        /// that repairs it. With <paramref name="breakBeforeStart"/> the output is broken from the start
        /// (a missing stream); otherwise a first row is delivered and the output breaks while the listener runs.
        /// <paramref name="readIdsAsync"/> returns the ids of the rows at the output and must not throw while
        /// the output is down.
        /// </summary>
        public static async Task RunAsync(string name,
                                          Func<PgOutput2JsonBuilder, PgOutput2JsonBuilder> useOutput,
                                          Func<Task<Func<Task>>> breakAsync,
                                          Func<Task<List<int>>> readIdsAsync,
                                          bool breakBeforeStart = false)
        {
            await Db.ExecuteAsync($@"
                CREATE TABLE {name} (id int PRIMARY KEY, v text);
                CREATE PUBLICATION {name}_pub FOR TABLE {name};");

            await Db.CreateSlotAsync($"{name}_slot");

            using var logs = new CollectingLoggerFactory();

            var builder = useOutput(PgOutput2JsonBuilder.Create()
                .WithLoggerFactory(logs)
                .WithPgConnectionString(Containers.PgConnectionString)
                .WithPgPublications($"{name}_pub")
                .WithPgReplicationSlot($"{name}_slot")
                .WithIdleWalMessageInterval(TimeSpan.FromMilliseconds(200)));

            Task<NpgsqlLogSequenceNumber> ConfirmedAsync() => Db.ScalarAsync<NpgsqlLogSequenceNumber>(
                $"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '{name}_slot'");

            Func<Task>? repairAsync = null;

            try
            {
                if (breakBeforeStart) repairAsync = await breakAsync();

                await using (new RunningListener(builder))
                {
                    if (!breakBeforeStart)
                    {
                        await Db.ExecuteAsync($"INSERT INTO {name} VALUES (1, 'before')");

                        await Wait.UntilAsync(async () => (await readIdsAsync()).Contains(1), "the row before the failure", 60);

                        var afterFirst = await Db.ScalarAsync<NpgsqlLogSequenceNumber>("SELECT pg_current_wal_insert_lsn()");

                        await Wait.UntilAsync(async () => await ConfirmedAsync() >= afterFirst, "the slot to move past the first row");

                        repairAsync = await breakAsync();
                    }

                    await Db.ExecuteAsync($"INSERT INTO {name} VALUES (2, 'during')");

                    var afterSecond = await Db.ScalarAsync<NpgsqlLogSequenceNumber>("SELECT pg_current_wal_insert_lsn()");

                    // the failure must reach the listener...
                    await Wait.UntilAsync(() => logs.Contains(LogLevel.Error, ""), "the failure to be logged", 90);

                    // ...and the row must not be confirmed: the listener reconnects and tries it again, so give it time to get that wrong
                    await Task.Delay(TimeSpan.FromSeconds(3));

                    var confirmed = await ConfirmedAsync();

                    Assert.IsTrue(confirmed < afterSecond, $"the row was confirmed to PostgreSQL while the output was down: {confirmed} >= {afterSecond}. Logged: {logs}");

                    await repairAsync!();
                    repairAsync = null;

                    var ids = new List<int>();

                    try
                    {
                        await Wait.UntilAsync(async () => (ids = await readIdsAsync()).Contains(2), "the row after the repair", 120);
                    }
                    catch (TimeoutException)
                    {
                        Assert.Fail($"The row did not arrive after the output was repaired. At the output: {string.Join(" ", ids)}. Logged: {logs}");
                    }

                    await Wait.UntilAsync(async () => await ConfirmedAsync() >= afterSecond, "the slot to move past the row", 60);
                }
            }
            finally
            {
                if (repairAsync != null) await repairAsync();
            }
        }

        /// <summary>
        /// The ids of the rows in the JSON messages.
        /// </summary>
        public static List<int> Ids(IEnumerable<string> messages)
        {
            return messages.Select(message =>
            {
                using var json = System.Text.Json.JsonDocument.Parse(message);

                var row = json.RootElement.GetProperty("r");

                return row.ValueKind == System.Text.Json.JsonValueKind.Array ? row[0].GetInt32() : row.GetProperty("id").GetInt32();
            }).ToList();
        }
    }
}
