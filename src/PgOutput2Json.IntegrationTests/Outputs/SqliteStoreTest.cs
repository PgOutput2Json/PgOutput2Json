using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

using Microsoft.Data.Sqlite;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace PgOutput2Json.IntegrationTests.Outputs
{
    [TestClass]
    public class SqliteStoreTest
    {
        [TestMethod]
        public async Task Sqlite_should_mirror_the_tables()
        {
            var path = Path.Combine(Path.GetTempPath(), $"pg2j_{Guid.NewGuid():N}.db");
            var connectionString = $"Data Source={path};Pooling=False";

            async Task<string> ReadAsync(string table)
            {
                await using var connection = new SqliteConnection(connectionString);
                await connection.OpenAsync();

                await using var command = connection.CreateCommand();

                command.CommandText = $"SELECT * FROM \"{table}\" ORDER BY id";

                var rows = new List<string>();

                try
                {
                    await using var reader = await command.ExecuteReaderAsync();

                    while (await reader.ReadAsync())
                    {
                        var values = new List<string>();

                        for (var i = 0; i < reader.FieldCount; i++)
                        {
                            var value = reader.GetValue(i);

                            values.Add(value switch
                            {
                                DBNull => "NULL",
                                byte[] bytes => "x" + Convert.ToHexString(bytes),
                                string text when text.Length > 100 => $"{text[0]}*{text.Length}",
                                _ => Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture)!,
                            });
                        }

                        rows.Add(string.Join("|", values));
                    }
                }
                catch (SqliteException)
                {
                    return "(no table)";
                }

                return string.Join("\n", rows);
            }

            try
            {
                await StoreScenario.RunAsync("os_sqlite",
                    builder => builder.UseSqlite(options => options.ConnectionStringBuilder = new SqliteConnectionStringBuilder(connectionString)),
                    ReadAsync,
                    expectedAfterCopy: "1|one|1.5|1|[\"a\",\"b c\"]|x0102|x*3000\n2|two|NULL|0|NULL|NULL|NULL",
                    expected: "1|uno|1.5|1|[\"a\",\"b c\"]|x0102|x*3000\n2|NULL|2.25|0|[\"x\"]|NULL|NULL\n30|three|3|NULL|[]|xFF|NULL",
                    expectedKeyLast: "changed|1\nnew|2",
                    expectedCompositeKey: "1|changed|x\n3|new|zz");
            }
            finally
            {
                File.Delete(path);
            }
        }

        /// <summary>
        /// A timestamp without time zone is stored without an offset, from the initial copy and from the stream, so it reads
        /// back as the same wall time on any machine (the CI runs this with TZ=Europe/Zagreb). timestamptz is stored in UTC
        /// with its offset, date, time and timetz as PostgreSQL's text.
        /// </summary>
        [TestMethod]
        public async Task Sqlite_should_store_timestamps_without_the_local_offset()
        {
            const string name = "os_sqlite_ts";

            var path = Path.Combine(Path.GetTempPath(), $"pg2j_{Guid.NewGuid():N}.db");
            var connectionString = $"Data Source={path};Pooling=False";

            async Task<string> ReadAsync()
            {
                await using var connection = new SqliteConnection(connectionString);
                await connection.OpenAsync();

                await using var command = connection.CreateCommand();

                command.CommandText = $"SELECT id, ts, tstz, d, t, ttz FROM \"{name}\" ORDER BY id";

                var rows = new List<string>();

                try
                {
                    await using var reader = await command.ExecuteReaderAsync();

                    while (await reader.ReadAsync())
                    {
                        var ts = reader.GetDateTime(1);

                        rows.Add($"{reader.GetInt64(0)}|{reader.GetString(1)}|{ts.Kind}|{reader.GetString(2)}|{reader.GetString(3)}|{reader.GetString(4)}|{reader.GetString(5)}");
                    }
                }
                catch (SqliteException)
                {
                    return "(no table)";
                }

                return string.Join("\n", rows);
            }

            await Db.ExecuteAsync($@"
                CREATE TABLE {name} (id int PRIMARY KEY, ts timestamp, tstz timestamptz, d date, t time, ttz timetz);
                INSERT INTO {name} VALUES (1, '2026-01-02 03:04:05.123456', '2026-01-02 03:04:05.123456+01', '2026-01-02', '03:04:05.123456', '03:04:05+02');
                CREATE PUBLICATION {name}_pub FOR TABLE {name};");

            await Db.CreateSlotAsync($"{name}_slot");

            var builder = PgOutput2JsonBuilder.Create()
                .WithPgConnectionString(Containers.PgConnectionString)
                .WithPgPublications($"{name}_pub")
                .WithPgReplicationSlot($"{name}_slot")
                .WithInitialDataCopy(true)
                .UseSqlite(options => options.ConnectionStringBuilder = new SqliteConnectionStringBuilder(connectionString));

            const string copied = "1|2026-01-02T03:04:05.1234560|Unspecified|2026-01-02T02:04:05.1234560+00:00|2026-01-02|03:04:05.123456|03:04:05+02";

            try
            {
                await using (new RunningListener(builder))
                {
                    var actual = "";

                    try
                    {
                        await Wait.UntilAsync(async () => (actual = await ReadAsync()) == copied, "the copied row");

                        await Db.ExecuteAsync($"INSERT INTO {name} VALUES (2, '2026-06-30 23:59:59', '2026-06-30 23:59:59+02', '2026-06-30', '23:59:59', '23:59:59-05')");

                        await Wait.UntilAsync(async () => (actual = await ReadAsync()) == copied + "\n2|2026-06-30T23:59:59.0000000|Unspecified|2026-06-30T21:59:59.0000000+00:00|2026-06-30|23:59:59|23:59:59-05", "the streamed row");
                    }
                    catch (TimeoutException e)
                    {
                        Assert.Fail($"{e.Message}. Stored: {actual}");
                    }
                }
            }
            finally
            {
                File.Delete(path);
            }
        }
    }
}
