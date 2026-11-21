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

            // CLICKHOUSE_ROWBINARY=true runs the tests with RowBinary instead of JSON, the default
            options.UseRowBinary = Environment.GetEnvironmentVariable("CLICKHOUSE_ROWBINARY") == "true";
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
        /// The current rows of the table as JSON arrays, without the version, the deleted flag and the sync time, ordered by <paramref name="orderBy"/>.
        /// A long run of x is shortened to x and its length.
        /// </summary>
        public static async Task<string> ReadAsync(string endpoint, string table, string orderBy = "id", bool final = true)
        {
            var result = await QueryAsync(endpoint, $"SELECT * EXCEPT (_version, _deleted, _synced_at) FROM `{table}`{(final ? " FINAL" : "")} ORDER BY {orderBy} FORMAT JSONCompactEachRow");

            if (result.Length == 0 || result == "(no table)") return result;

            return string.Join("\n", result.Split('\n').Select(line =>
            {
                using var json = JsonDocument.Parse(line);

                var row = "[" + string.Join(",", json.RootElement.EnumerateArray().Select(value => value.GetRawText())) + "]";

                return System.Text.RegularExpressions.Regex.Replace(row, "x{100,}", match => $"x*{match.Length}");
            }));
        }

        [TestMethod]
        public async Task ClickHouse_should_mirror_the_tables()
        {
            var endpoint = await Containers.GetClickHouseEndpointAsync();

            // arrays are ClickHouse arrays (a null one is empty) and bytea is hex
            await StoreScenario.RunAsync("os_click",
                builder => builder.UseClickHouse(options => Configure(options, endpoint)),
                table => ReadAsync(endpoint, table),
                expectedAfterCopy:
                    """
                    [1,"one",1.5,true,["a","b c"],"0102","x*3000"]
                    [2,"two",null,false,[],null,null]
                    """,
                expected:
                    """
                    [1,"uno",1.5,true,["a","b c"],"0102","x*3000"]
                    [2,null,2.25,false,["x"],null,null]
                    [30,"three",3,null,[],"ff",null]
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
        private static async Task<string> RunAsync(string name, string tables, Func<string, Task> changeAsync, Action<ClickHousePublisherOptions>? configure = null)
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
                .UseClickHouse(options =>
                {
                    Configure(options, endpoint);
                    configure?.Invoke(options);
                });

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
        public async Task Existing_table_should_get_RowBinary_for_the_types_it_covers_and_JSON_for_others()
        {
            var endpoint = await Containers.GetClickHouseEndpointAsync();

            // made before the first row, with other types than the output creates: covered by RowBinary, and not (Map)
            await QueryAsync(endpoint, "CREATE TABLE clk_own_bin (id Int64, name LowCardinality(String), at DateTime64(3, 'UTC'), at_s DateTime('UTC'), d Date, " +
                "mood Nullable(Enum8('sad' = 1, 'happy' = 2)), code FixedString(4), ip4 IPv4, ip6 IPv6, _version UInt128, _deleted UInt8, " +
                "_synced_at DateTime64(6, 'UTC') DEFAULT now64(6)) ENGINE = ReplacingMergeTree(_version, _deleted) ORDER BY id");
            await QueryAsync(endpoint, "CREATE TABLE clk_own_json (id Int32, name String, n Int128, tags Map(String, String), _version UInt128, _deleted UInt8, " +
                "_synced_at DateTime64(6, 'UTC') DEFAULT now64(6)) ENGINE = ReplacingMergeTree(_version, _deleted) ORDER BY id");

            await Db.ExecuteAsync(@"
                CREATE TYPE clk_own_mood AS ENUM ('sad', 'happy');
                CREATE TABLE clk_own_bin (id int PRIMARY KEY, name text, at timestamptz, at_s timestamptz, d date, mood clk_own_mood, code char(4), ip4 inet, ip6 inet);
                CREATE TABLE clk_own_json (id int PRIMARY KEY, name text, n bigint, tags jsonb);");

            await RunAsync("clk_own", "clk_own_bin, clk_own_json", async endpoint =>
            {
                await Db.ExecuteAsync(@"
                    INSERT INTO clk_own_bin VALUES
                        (1, 'a', '2024-02-29 12:34:56.789+02', '2024-02-29 12:34:56.789+02', '2024-02-29', 'happy', 'ab', '10.0.0.1', '2001:db8::1'),
                        (2, NULL, '1999-01-01 00:00:00+00', '1900-01-01 00:00:00+00', '9999-12-31', 'sad', 'abcd', '192.168.1.2', '10.0.0.2'),
                        (3, 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL);
                    UPDATE clk_own_bin SET name = 'b' WHERE id = 2;
                    DELETE FROM clk_own_bin WHERE id = 3;
                    INSERT INTO clk_own_json VALUES (1, 'a', 9223372036854775807, '{""k"": ""v""}'), (2, 'b', 2, '{}');
                    DELETE FROM clk_own_json WHERE id = 2;");

                // a null for a column that is not Nullable is the default, as with JSON; a date out of range is the first or the last one;
                // char(4) is padded with spaces by PostgreSQL; an IPv4 address in an IPv6 column is ::ffff:a.b.c.d
                await WaitForAsync(endpoint, "clk_own_bin",
                    """
                    [1,"a","2024-02-29 10:34:56.789","2024-02-29 10:34:56","2024-02-29","happy","ab  ","10.0.0.1","2001:db8::1"]
                    [2,"b","1999-01-01 00:00:00.000","1970-01-01 00:00:00","2149-06-06","sad","abcd","192.168.1.2","::ffff:10.0.0.2"]
                    """);

                await WaitForAsync(endpoint, "clk_own_json", """[1,"a",9223372036854775807,{"k":"v"}]""");
            });

            await QueryAsync(endpoint, "SYSTEM FLUSH LOGS");

            async Task<string> FormatsAsync(string table) => await QueryAsync(endpoint,
                "SELECT arrayStringConcat(groupUniqArray(extract(query, 'FORMAT (\\w+)')), ',') FROM system.query_log " +
                $"WHERE type = 'QueryFinish' AND query_kind = 'Insert' AND query LIKE '%`{table}`%' FORMAT TSVRaw");

            var rowBinary = Environment.GetEnvironmentVariable("CLICKHOUSE_ROWBINARY") == "true";

            Assert.AreEqual(rowBinary ? "RowBinary" : "JSONCompactEachRow", await FormatsAsync("clk_own_bin"));
            Assert.AreEqual("JSONCompactEachRow", await FormatsAsync("clk_own_json"));
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
                        '2024-02-29 12:34:56.789+02', '12:34:56', '1 day', '{""a"": 1}', '{1,NULL,3}', 'happy');
                    INSERT INTO clk_types (id, num, f8, d, ts, tstz) VALUES (2, 'NaN', 'NaN', 'infinity', '-infinity', 'infinity');
                    INSERT INTO clk_types (id, d, ts, tstz) VALUES (3, '0044-03-15 BC', '10000-01-01 00:00:00', '0044-03-15 12:00:00+00 BC');");

                // NaN is null, as in the JSON; a date ClickHouse cannot store is the first or the last one it can
                await WaitForAsync(endpoint, "clk_types",
                    """
                    [1,-2,9223372036854775807,12345678901234567890.123456789,1.5,1.5,1e300,true,"deadbeef","quote\" back\\ nl\n tab\t unié 😀","a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11","2024-02-29","2024-02-29 12:34:56.789000","2024-02-29 10:34:56.789000","12:34:56","1 day",{"a":1},[1,null,3],"happy"]
                    [2,null,null,null,null,null,null,null,null,null,null,"9999-12-31","0001-01-01 00:00:00.000000","9999-12-31 23:59:59.999999",null,null,null,[],null]
                    [3,null,null,null,null,null,null,null,null,null,null,"0001-01-01","9999-12-31 23:59:59.999999","0001-01-01 00:00:00.000000",null,null,null,[],null]
                    """);
            });

            Assert.AreEqual(
                "id Int32, i2 Nullable(Int16), i8 Nullable(Int64), num Nullable(Decimal(76, 38)), n2 Nullable(Decimal(12, 4)), f4 Nullable(Float32), " +
                "f8 Nullable(Float64), b Nullable(Bool), by Nullable(String), t Nullable(String), u Nullable(UUID), d Nullable(Date32), " +
                "ts Nullable(DateTime64(6, 'UTC')), tstz Nullable(DateTime64(6, 'UTC')), tm Nullable(String), iv Nullable(String), j Nullable(JSON), " +
                "a Array(Nullable(Int32)), mood Nullable(String), _version UInt128, _deleted UInt8, _synced_at DateTime64(6, 'UTC')",
                await QueryAsync(endpoint, $"SELECT arrayStringConcat(groupArray(concat(name, ' ', type)), ', ') FROM (SELECT name, type FROM system.columns WHERE database = '{Containers.ClickHouseDatabase}' AND table = 'clk_types' ORDER BY position) FORMAT TSVRaw"));
        }

        /// <summary>
        /// A timestamp without time zone is a wall time of TimestampTimeZone: its column is created with that time zone, so
        /// ClickHouse stores the same moment whatever the time zone of the server. 2026-01-01 00:00 in Zagreb is
        /// 2025-12-31 23:00 UTC; a timestamptz is not affected.
        /// </summary>
        [TestMethod]
        public async Task Timestamp_should_be_stored_as_a_time_of_the_timestamp_time_zone()
        {
            await Db.ExecuteAsync("CREATE TABLE clk_tz (id int PRIMARY KEY, ts timestamp, tstz timestamptz, tsa timestamp[])");

            var endpoint = await RunAsync("clk_tz", "clk_tz", async endpoint =>
            {
                await Db.ExecuteAsync("INSERT INTO clk_tz VALUES (1, '2026-01-01 00:00:00', '2026-01-01 00:00:00+00', '{\"2026-07-01 00:00:00\"}')");

                await WaitForAsync(endpoint, "clk_tz", """[1,"2026-01-01 00:00:00.000000","2026-01-01 00:00:00.000000",["2026-07-01 00:00:00.000000"]]""");
            },
            options => options.TimestampTimeZone = TimeZoneInfo.FindSystemTimeZoneById("Europe/Zagreb"));

            Assert.AreEqual("1767222000000000 1767225600000000 1782856800000000",
                await QueryAsync(endpoint, "SELECT concat(toString(toUnixTimestamp64Micro(ts)), ' ', toString(toUnixTimestamp64Micro(tstz)), ' ', " +
                    "toString(toUnixTimestamp64Micro(tsa[1]))) FROM clk_tz FINAL FORMAT TSVRaw"));

            Assert.AreEqual("ts Nullable(DateTime64(6, 'Europe/Zagreb')), tsa Array(Nullable(DateTime64(6, 'Europe/Zagreb')))",
                await QueryAsync(endpoint, $"SELECT arrayStringConcat(groupArray(concat(name, ' ', type)), ', ') FROM (SELECT name, type FROM system.columns " +
                    $"WHERE database = '{Containers.ClickHouseDatabase}' AND table = 'clk_tz' AND name IN ('ts', 'tsa') ORDER BY position) FORMAT TSVRaw"));
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
        public async Task Deleted_rows_should_be_removed_with_the_cleanup_and_every_row_should_have_its_sync_time()
        {
            await Db.ExecuteAsync(@"
                CREATE TABLE clk_clean (id int PRIMARY KEY, v text);
                CREATE TABLE clk_clean_new (id int PRIMARY KEY, v text);");

            // without the cleanup the table is created as before, and a deleted row stays as a row marked deleted
            var endpoint = await RunAsync("clk_clean_a", "clk_clean", async endpoint =>
            {
                await Db.ExecuteAsync("INSERT INTO clk_clean VALUES (1, 'a'), (2, 'b'), (3, 'c'); DELETE FROM clk_clean WHERE id = 1;");

                await WaitForAsync(endpoint, "clk_clean",
                    """
                    [2,"b"]
                    [3,"c"]
                    """);
            });

            Assert.AreEqual("0", await QueryAsync(endpoint, $"SELECT position(create_table_query, 'allow_experimental_replacing_merge_with_cleanup') FROM system.tables WHERE database = '{Containers.ClickHouseDatabase}' AND name = 'clk_clean'"));

            // with it, the existing table gets the settings on the next start, and a new table when it is created
            await RunAsync("clk_clean_b", "clk_clean, clk_clean_new", async endpoint =>
            {
                await Db.ExecuteAsync(@"
                    DELETE FROM clk_clean WHERE id = 2;
                    INSERT INTO clk_clean_new VALUES (1, 'a'), (2, 'b');
                    DELETE FROM clk_clean_new WHERE id = 1;");

                await WaitForAsync(endpoint, "clk_clean", """[3,"c"]""");
                await WaitForAsync(endpoint, "clk_clean_new", """[2,"b"]""");

                // no new rows for a second: a merge removes the rows marked deleted
                foreach (var table in new[] { "clk_clean", "clk_clean_new" })
                {
                    await Wait.UntilAsync(async () => await QueryAsync(endpoint, $"SELECT count() FROM `{table}` WHERE _deleted = 1") == "0", $"the cleanup of {table}", 60);
                }
            }, options => options.CleanUpDeletedRowsAfter = TimeSpan.FromSeconds(1));

            foreach (var table in new[] { "clk_clean", "clk_clean_new" })
            {
                StringAssert.Contains(await QueryAsync(endpoint, $"SELECT create_table_query FROM system.tables WHERE database = '{Containers.ClickHouseDatabase}' AND name = '{table}'"),
                    "allow_experimental_replacing_merge_with_cleanup = 1", table);
            }

            // every row has the time ClickHouse wrote it
            Assert.AreEqual("1", await QueryAsync(endpoint, "SELECT min(_synced_at) > now64(6) - INTERVAL 10 MINUTE AND max(_synced_at) <= now64(6) FROM clk_clean"));
            Assert.AreEqual("DateTime64(6, 'UTC')", await QueryAsync(endpoint, "SELECT toTypeName(_synced_at) FROM clk_clean LIMIT 1 FORMAT TSVRaw"));
        }

        [TestMethod]
        public async Task Table_options_should_set_the_order_the_partitions_and_the_engine()
        {
            await Db.ExecuteAsync(@"
                CREATE TABLE clk_opt_order (id int, tenant int, v text, PRIMARY KEY (id, tenant));
                CREATE TABLE clk_opt_history (id int PRIMARY KEY, big text, v int);
                ALTER TABLE clk_opt_history ALTER COLUMN big SET STORAGE EXTERNAL;
                CREATE TABLE clk_opt_null (id int PRIMARY KEY, v text);");

            var endpoint = await RunAsync("clk_opt", "clk_opt_order, clk_opt_history, clk_opt_null", async endpoint =>
            {
                await Db.ExecuteAsync(@"
                    INSERT INTO clk_opt_order VALUES (1, 100, 'a'), (2, 200, 'b'), (3, 200, 'c');
                    UPDATE clk_opt_order SET v = 'bb' WHERE id = 2;
                    DELETE FROM clk_opt_order WHERE id = 3;
                    INSERT INTO clk_opt_history VALUES (1, repeat('x', 3000), 1);
                    UPDATE clk_opt_history SET v = 2;
                    DELETE FROM clk_opt_history;
                    INSERT INTO clk_opt_null VALUES (1, 'a');");

                await WaitForAsync(endpoint, "clk_opt_order",
                    """
                    [1,100,"a"]
                    [2,200,"bb"]
                    """);

                // every version, the delete too, and the large value of the update read from the table without FINAL
                await Wait.UntilAsync(async () => await QueryAsync(endpoint, "SELECT count() FROM clk_opt_history") == "3", "the history");

                Assert.AreEqual("[1, \"x*3000\", 1, 0]\n[1, \"x*3000\", 2, 0]\n[1, null, null, 1]",
                    System.Text.RegularExpressions.Regex.Replace(
                        await QueryAsync(endpoint, "SELECT id, big, v, _deleted FROM clk_opt_history ORDER BY _version FORMAT JSONCompactEachRow"),
                        "x{100,}", match => $"x*{match.Length}"));

                // a new column for a Null table
                await Db.ExecuteAsync("ALTER TABLE clk_opt_null ADD COLUMN extra int; INSERT INTO clk_opt_null VALUES (2, 'b', 9);");

                await Wait.UntilAsync(async () => (await QueryAsync(endpoint, "SELECT count() FROM system.columns WHERE table = 'clk_opt_null' AND name = 'extra'")) == "1", "the new column");
            }, options =>
            {
                options.Tables["public.clk_opt_order"] = new() { OrderBy = ["tenant", "id"], PartitionBy = "intDiv(tenant, 100)" };
                options.Tables["public.clk_opt_history"] = new() { Engine = ClickHouseTableEngine.MergeTree };
                options.Tables["public.clk_opt_null"] = new() { Engine = ClickHouseTableEngine.Null };
            });

            Assert.AreEqual(
                "clk_opt_history MergeTree  (id)\nclk_opt_null Null  \nclk_opt_order ReplacingMergeTree intDiv(tenant, 100) tenant, id",
                await QueryAsync(endpoint, $"SELECT name, engine, partition_key, sorting_key FROM system.tables WHERE database = '{Containers.ClickHouseDatabase}' AND name LIKE 'clk_opt_%' ORDER BY name FORMAT TSVRaw").ContinueWith(t => t.Result.Replace('\t', ' ')));

            Assert.AreEqual("0", await QueryAsync(endpoint, "SELECT count() FROM clk_opt_null"));
        }

        [TestMethod]
        public async Task Arrays_should_be_ClickHouse_arrays_or_their_JSON_text_without_the_array_types()
        {
            const string columns = "(id int PRIMARY KEY, tags text[], days date[], at timestamptz[], amounts numeric(10,2)[], ids uuid[], flags bool[], f float8[], moods clk_arr_mood[], ints clk_arr_ints)";
            const string values = @"
                (1, '{a,NULL,""b c""}', '{2024-02-29,infinity,0044-03-15 BC}', '{2024-02-29 12:00:00+02}', '{1.50,NULL}',
                    '{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}', '{t,f}', '{1.5,NaN}', '{sad,""so so"",NULL}', '{1,2}'),
                (2, '{}', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)";

            await Db.ExecuteAsync($@"
                CREATE TYPE clk_arr_mood AS ENUM ('sad', 'so so');
                CREATE DOMAIN clk_arr_ints AS int[];
                CREATE TABLE clk_arr {columns};
                CREATE TABLE clk_arr_text {columns};");

            // a date ClickHouse cannot store is the nearest one, NaN is null, a null array is empty, and an array of an enum, or a domain over an array, is an array of strings
            var endpoint = await RunAsync("clk_arr", "clk_arr", async endpoint =>
            {
                await Db.ExecuteAsync($"INSERT INTO clk_arr VALUES {values}");

                await WaitForAsync(endpoint, "clk_arr",
                    """
                    [1,["a",null,"b c"],["2024-02-29","9999-12-31","0001-01-01"],["2024-02-29 10:00:00.000000"],[1.5,null],["a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"],[true,false],[1.5,null],["sad","so so",null],["1","2"]]
                    [2,[],[],[],[],[],[],[],[],[]]
                    """);
            });

            Assert.AreEqual(
                "tags Array(Nullable(String)), days Array(Nullable(Date32)), at Array(Nullable(DateTime64(6, 'UTC'))), amounts Array(Nullable(Decimal(10, 2))), " +
                "ids Array(Nullable(UUID)), flags Array(Nullable(Bool)), f Array(Nullable(Float64)), moods Array(Nullable(String)), ints Array(Nullable(String))",
                await QueryAsync(endpoint, $"SELECT arrayStringConcat(groupArray(concat(name, ' ', type)), ', ') FROM (SELECT name, type FROM system.columns WHERE database = '{Containers.ClickHouseDatabase}' AND table = 'clk_arr' AND name NOT IN ('id', '_version', '_deleted', '_synced_at') ORDER BY position) FORMAT TSVRaw"));

            // without the array types, the JSON text: a null array stays null
            await RunAsync("clk_arr_text", "clk_arr_text", async endpoint =>
            {
                await Db.ExecuteAsync($"INSERT INTO clk_arr_text VALUES {values}");

                await WaitForAsync(endpoint, "clk_arr_text",
                    """
                    [1,"[\"a\",null,\"b c\"]","[\"2024-02-29\",\"infinity\",\"0044-03-15 BC\"]","[\"2024-02-29 10:00:00+00\"]","[1.50,null]","[\"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\"]","[true,false]","[1.5,null]","{sad,\"so so\",NULL}","{1,2}"]
                    [2,"[]",null,null,null,null,null,null,null,null]
                    """);
            }, options => options.UseArrayTypes = false);
        }

        [TestMethod]
        public async Task Multi_dimensional_array_should_stop_until_the_column_is_a_String()
        {
            var endpoint = await Containers.GetClickHouseEndpointAsync();

            await Db.ExecuteAsync(@"
                CREATE TABLE clk_arr_dim (id int PRIMARY KEY, a int[]);
                CREATE PUBLICATION clk_arr_dim_pub FOR TABLE clk_arr_dim;");

            await Db.CreateSlotAsync("clk_arr_dim_slot");

            using var logs = new CollectingLoggerFactory();

            var builder = PgOutput2JsonBuilder.Create()
                .WithLoggerFactory(logs)
                .WithPgConnectionString(Containers.PgConnectionString)
                .WithPgPublications("clk_arr_dim_pub")
                .WithPgReplicationSlot("clk_arr_dim_slot")
                .WithReconnectDelay(TimeSpan.FromMilliseconds(500))
                .UseClickHouse(options => Configure(options, endpoint));

            await using (new RunningListener(builder))
            {
                await Db.ExecuteAsync("INSERT INTO clk_arr_dim VALUES (1, '{1,2}')");

                await WaitForAsync(endpoint, "clk_arr_dim", "[1,[1,2]]");

                await Db.ExecuteAsync("INSERT INTO clk_arr_dim VALUES (2, '{{1,2},{3,4}}')");

                await Wait.UntilAsync(() => logs.Contains(Microsoft.Extensions.Logging.LogLevel.Error, "Column a has a multi-dimensional array"), "the error");

                // the fix the error names: the listener reconnects, sees the String and writes the JSON text
                await QueryAsync(endpoint, "ALTER TABLE clk_arr_dim MODIFY COLUMN a String");

                await WaitForAsync(endpoint, "clk_arr_dim",
                    """
                    [1,"[1,2]"]
                    [2,"[[1,2],[3,4]]"]
                    """);
            }
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

        [TestMethod]
        public async Task Json_should_be_stored_in_the_JSON_type_with_other_values_than_objects_wrapped()
        {
            await Db.ExecuteAsync(@"
                CREATE TABLE clk_json (id int PRIMARY KEY, j json, jb jsonb, v int);
                ALTER TABLE clk_json ALTER COLUMN jb SET STORAGE EXTERNAL;");

            await RunAsync("clk_json", "clk_json", async endpoint =>
            {
                // json keeps its text, with the line break; row 6 has a jsonb too large to be sent again with an update;
                // row 7 has what ClickHouse would reject: a key twice, a number above the largest double and two keys with the same path
                await Db.ExecuteAsync(@"
                    INSERT INTO clk_json VALUES
                        (1, E'{""a"": 1,\n ""b"": {""c"": [1, ""x""]}}', '{""a"": 1, ""t"": ""quote\"" é""}', 1),
                        (2, '[1, 2]', '[{""a"": 1}]', 1),
                        (3, '5', '""text""', 1),
                        (4, 'null', 'true', 1),
                        (5, NULL, NULL, 1),
                        (6, '{}', jsonb_build_object('big', repeat('x', 3000)), 1),
                        (7, '{""a"": 1, ""a"": 2, ""n"": 1e400, ""o"": {""a.b"": 1, ""a"": {""b"": 2}}}', NULL, 1);
                    UPDATE clk_json SET v = 2 WHERE id = 6;");

                await WaitForAsync(endpoint, "clk_json",
                    """
                    [1,{"a":1,"b":{"c":[1,"x"]}},{"a":1,"t":"quote\" é"},1]
                    [2,{"v":[1,2]},{"v":[{"a":1}]},1]
                    [3,{"v":5},{"v":"text"},1]
                    [4,{},{"v":true},1]
                    [5,null,null,1]
                    [6,{},{"big":"x*3000"},2]
                    [7,{"a":2,"n":"1e400","o":{"a":{"b":1}}},null,1]
                    """);
            });
        }

        [TestMethod]
        public async Task Json_should_be_stored_as_text_without_the_JSON_type()
        {
            await Db.ExecuteAsync("CREATE TABLE clk_jtext (id int PRIMARY KEY, jb jsonb)");

            var endpoint = await RunAsync("clk_jtext", "clk_jtext", async endpoint =>
            {
                await Db.ExecuteAsync(@"INSERT INTO clk_jtext VALUES (1, '{""a"": 1}'), (2, '[1, 2]'), (3, NULL)");

                await WaitForAsync(endpoint, "clk_jtext",
                    """
                    [1,"{\"a\": 1}"]
                    [2,"[1, 2]"]
                    [3,null]
                    """);
            },
            options => options.UseJsonType = false);

            Assert.AreEqual("Nullable(String)", await QueryAsync(endpoint, $"SELECT type FROM system.columns WHERE database = '{Containers.ClickHouseDatabase}' AND table = 'clk_jtext' AND name = 'jb'"));
        }
    }
}
