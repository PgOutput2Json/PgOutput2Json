using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using PgOutput2Json.ClickHouse;

namespace PgOutput2Json.IntegrationTests.Outputs
{
    [TestClass]
    public class ClickHouseStoreTest
    {
        private static readonly HttpClient Http = new();

        public static void Configure(ClickHousePublisherOptions options, string endpoint)
        {
            options.Endpoint = endpoint;
            options.Database = Containers.ClickHouseDatabase;
            options.Username = Containers.ClickHouseUsername;
            options.Password = Containers.ClickHousePassword;
        }

        /// <summary>
        /// Runs a statement and returns the answer, or "(no table)" if it reads a table that does not exist.
        /// </summary>
        public static async Task<string> QueryAsync(string endpoint, string sql)
        {
            using var request = new HttpRequestMessage(HttpMethod.Post, $"{endpoint}/?database={Containers.ClickHouseDatabase}")
            {
                Content = new StringContent(sql),
            };

            request.Headers.Add("X-ClickHouse-User", Containers.ClickHouseUsername);
            request.Headers.Add("X-ClickHouse-Key", Containers.ClickHousePassword);

            using var response = await Http.SendAsync(request);

            var body = await response.Content.ReadAsStringAsync();

            if (response.IsSuccessStatusCode) return body.TrimEnd();

            // UNKNOWN_TABLE
            if (response.Headers.TryGetValues("X-ClickHouse-Exception-Code", out var codes) && codes.First() == "60") return "(no table)";

            throw new InvalidOperationException($"ClickHouse: {body}");
        }

        /// <summary>
        /// The current rows of the table as JSON arrays, without the version and the deleted flag, ordered by <paramref name="orderBy"/>.
        /// A long text is shortened to its first character and its length.
        /// </summary>
        public static async Task<string> ReadAsync(string endpoint, string table, string orderBy = "id", bool final = true)
        {
            var result = await QueryAsync(endpoint, $"SELECT * EXCEPT (_version, _deleted) FROM `{table}`{(final ? " FINAL" : "")} ORDER BY {orderBy} FORMAT JSONCompactEachRow");

            if (result.Length == 0 || result == "(no table)") return result;

            return string.Join("\n", result.Split('\n').Select(line =>
            {
                using var json = JsonDocument.Parse(line);

                return "[" + string.Join(",", json.RootElement.EnumerateArray().Select(value =>
                    value.ValueKind == JsonValueKind.String && value.GetString()!.Length > 100
                        ? $"\"{value.GetString()![0]}*{value.GetString()!.Length}\""
                        : value.GetRawText())) + "]";
            }));
        }

        [TestMethod]
        public async Task ClickHouse_should_mirror_the_tables()
        {
            var endpoint = await Containers.GetClickHouseEndpointAsync();

            // arrays are stored as their JSON text and bytea as hex
            await StoreScenario.RunAsync("os_click",
                builder => builder.UseClickHouse(options => Configure(options, endpoint)),
                table => ReadAsync(endpoint, table),
                expectedAfterCopy:
                    """
                    [1,"one",1.5,true,"[\"a\",\"b c\"]","0102","x*3000"]
                    [2,"two",null,false,null,null,null]
                    """,
                expected:
                    """
                    [1,"uno",1.5,true,"[\"a\",\"b c\"]","0102","x*3000"]
                    [2,null,2.25,false,"[\"x\"]",null,null]
                    [30,"three",3,null,"[]","ff",null]
                    """,
                expectedKeyLast:
                    """
                    ["changed",1]
                    ["new",2]
                    """,
                expectedCompositeKey:
                    """
                    [1,"changed","x"]
                    [3,"new","zz"]
                    """);
        }

        /// <summary>
        /// Runs a listener with ClickHouse as the output on a publication of the tables, while <paramref name="changeAsync"/> runs.
        /// </summary>
        private static async Task<string> RunAsync(string name, string tables, Func<string, Task> changeAsync)
        {
            var endpoint = await Containers.GetClickHouseEndpointAsync();

            await Db.ExecuteAsync($"CREATE PUBLICATION {name}_pub FOR TABLE {tables}");
            await Db.CreateSlotAsync($"{name}_slot");

            using var logs = new CollectingLoggerFactory();

            var builder = PgOutput2JsonBuilder.Create()
                .WithLoggerFactory(logs)
                .WithPgConnectionString(Containers.PgConnectionString)
                .WithPgPublications($"{name}_pub")
                .WithPgReplicationSlot($"{name}_slot")
                .UseClickHouse(options => Configure(options, endpoint));

            await using (new RunningListener(builder))
            {
                await changeAsync(endpoint);
            }

            Assert.IsFalse(logs.Contains(Microsoft.Extensions.Logging.LogLevel.Error, ""), $"errors were logged: {logs}");

            return endpoint;
        }

        private static async Task WaitForAsync(string endpoint, string table, string expected, string orderBy = "id", bool final = true)
        {
            expected = expected.ReplaceLineEndings("\n");

            var actual = "";

            try
            {
                await Wait.UntilAsync(async () => (actual = await ReadAsync(endpoint, table, orderBy, final)) == expected, $"the rows of {table}");
            }
            catch (TimeoutException)
            {
                Assert.AreEqual(expected, actual, table);
            }
        }

        [TestMethod]
        public async Task Types_should_get_a_matching_ClickHouse_type_and_dates_out_of_range_the_nearest_date()
        {
            await Db.ExecuteAsync(@"
                CREATE TYPE clk_mood AS ENUM ('sad', 'happy');
                CREATE TABLE clk_types (id int PRIMARY KEY, i2 smallint, i8 bigint, num numeric, n2 numeric(12,4), f4 real, f8 float8, b bool,
                    by bytea, t text, u uuid, d date, ts timestamp, tstz timestamptz, tm time, iv interval, j jsonb, a int[], mood clk_mood);");

            var endpoint = await RunAsync("clk_types", "clk_types", async endpoint =>
            {
                await Db.ExecuteAsync(@"
                    INSERT INTO clk_types VALUES (1, -2, 9223372036854775807, 12345678901234567890.123456789, 1.5, 1.5, 1e300, true,
                        '\xDEADBEEF', E'quote"" back\\ nl\n tab\t unié 😀', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '2024-02-29', '2024-02-29 12:34:56.789',
                        '2024-02-29 12:34:56.789+02', '12:34:56', '1 day', '{""a"": 1}', '{{1,2},{3,4}}', 'happy');
                    INSERT INTO clk_types (id, num, f8, d, ts, tstz) VALUES (2, 'NaN', 'NaN', 'infinity', '-infinity', 'infinity');
                    INSERT INTO clk_types (id, d, ts, tstz) VALUES (3, '0044-03-15 BC', '10000-01-01 00:00:00', '0044-03-15 12:00:00+00 BC');");

                // NaN is null, as in the JSON; a date ClickHouse cannot store is the first or the last one it can
                await WaitForAsync(endpoint, "clk_types",
                    """
                    [1,-2,9223372036854775807,12345678901234567890.123456789,1.5,1.5,1e300,true,"deadbeef","quote\" back\\ nl\n tab\t unié 😀","a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11","2024-02-29","2024-02-29 12:34:56.789000","2024-02-29 10:34:56.789000","12:34:56","1 day","{\"a\": 1}","[[1,2],[3,4]]","happy"]
                    [2,null,null,null,null,null,null,null,null,null,null,"9999-12-31","0001-01-01 00:00:00.000000","9999-12-31 23:59:59.999999",null,null,null,null,null]
                    [3,null,null,null,null,null,null,null,null,null,null,"0001-01-01","9999-12-31 23:59:59.999999","0001-01-01 00:00:00.000000",null,null,null,null,null]
                    """);
            });

            Assert.AreEqual(
                "id Int32, i2 Nullable(Int16), i8 Nullable(Int64), num Nullable(Decimal(76, 38)), n2 Nullable(Decimal(12, 4)), f4 Nullable(Float32), " +
                "f8 Nullable(Float64), b Nullable(Bool), by Nullable(String), t Nullable(String), u Nullable(UUID), d Nullable(Date32), " +
                "ts Nullable(DateTime64(6)), tstz Nullable(DateTime64(6, 'UTC')), tm Nullable(String), iv Nullable(String), j Nullable(String), " +
                "a Nullable(String), mood Nullable(String), _version UInt128, _deleted UInt8",
                await QueryAsync(endpoint, $"SELECT arrayStringConcat(groupArray(concat(name, ' ', type)), ', ') FROM (SELECT name, type FROM system.columns WHERE database = '{Containers.ClickHouseDatabase}' AND table = 'clk_types' ORDER BY position) FORMAT TSVRaw"));
        }

        [TestMethod]
        public async Task Table_with_full_replica_identity_should_follow_updates_and_deletes()
        {
            await Db.ExecuteAsync(@"
                CREATE TABLE clk_full (a int, b text, big text);
                ALTER TABLE clk_full ALTER COLUMN big SET STORAGE EXTERNAL;
                ALTER TABLE clk_full REPLICA IDENTITY FULL;");

            await RunAsync("clk_full", "clk_full", async endpoint =>
            {
                // every column is in the key, so the old row of an update is deleted and the new one inserted
                await Db.ExecuteAsync(@"
                    INSERT INTO clk_full VALUES (1, 'one', repeat('x', 3000)), (NULL, 'null', NULL), (3, 'three', NULL), (4, 'same', NULL);
                    UPDATE clk_full SET b = 'uno' WHERE a = 1;         -- big is not sent
                    UPDATE clk_full SET b = NULL WHERE a IS NULL;
                    UPDATE clk_full SET b = 'same' WHERE a = 4;        -- nothing changes
                    DELETE FROM clk_full WHERE a = 3;");

                await WaitForAsync(endpoint, "clk_full",
                    """
                    [1,"uno","x*3000"]
                    [4,"same",null]
                    [null,null,null]
                    """, "a");
            });
        }

        [TestMethod]
        public async Task Table_without_a_key_and_a_table_of_another_schema_should_get_their_rows()
        {
            await Db.ExecuteAsync(@"
                CREATE TABLE clk_nokey (a int, b text);
                CREATE SCHEMA clk_s;
                CREATE TABLE clk_s.items (id int PRIMARY KEY, v text);");

            var endpoint = await RunAsync("clk_nokey", "clk_nokey, clk_s.items", async endpoint =>
            {
                await Db.ExecuteAsync(@"
                    INSERT INTO clk_nokey VALUES (1, 'a'), (1, 'a'), (2, NULL);
                    INSERT INTO clk_s.items VALUES (1, 'a');
                    UPDATE clk_s.items SET v = 'b';");

                // nothing replaces a row of a table without a key: the same row twice stays twice, and the table is read without FINAL
                await WaitForAsync(endpoint, "clk_nokey",
                    """
                    [1,"a"]
                    [1,"a"]
                    [2,null]
                    """, "a", final: false);

                await WaitForAsync(endpoint, "clk_s_items", """[1,"b"]""");
            });

            Assert.AreEqual("clk_nokey MergeTree, clk_s_items ReplacingMergeTree",
                await QueryAsync(endpoint, $"SELECT arrayStringConcat(groupArray(concat(name, ' ', engine)), ', ') FROM (SELECT name, engine FROM system.tables WHERE database = '{Containers.ClickHouseDatabase}' AND name IN ('clk_nokey', 'clk_s_items') ORDER BY name)"));
        }

        [TestMethod]
        public async Task New_column_and_a_changed_key_should_keep_the_large_value()
        {
            await Db.ExecuteAsync(@"
                CREATE TABLE clk_alter (code text PRIMARY KEY, big text, v int);
                ALTER TABLE clk_alter ALTER COLUMN big SET STORAGE EXTERNAL;");

            await RunAsync("clk_alter", "clk_alter", async endpoint =>
            {
                // the large value is read from ClickHouse by the key, which has every character that needs escaping
                await Db.ExecuteAsync(@"
                    INSERT INTO clk_alter VALUES (E'a\tb''c\\d""e\nf`g', repeat('x', 3000), 1);
                    UPDATE clk_alter SET v = 2;");

                await Db.ExecuteAsync("UPDATE clk_alter SET code = 'new'");

                await WaitForAsync(endpoint, "clk_alter", """["new","x*3000",2]""", "code");

                await Db.ExecuteAsync(@"
                    ALTER TABLE clk_alter ADD COLUMN extra int;
                    INSERT INTO clk_alter VALUES ('z', NULL, 3, 9);
                    UPDATE clk_alter SET extra = 8 WHERE code = 'new';");

                await WaitForAsync(endpoint, "clk_alter",
                    """
                    ["new","x*3000",2,8]
                    ["z",null,3,9]
                    """, "code");
            });
        }
    }
}
