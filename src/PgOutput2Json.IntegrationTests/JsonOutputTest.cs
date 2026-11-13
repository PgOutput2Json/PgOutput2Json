using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace PgOutput2Json.IntegrationTests
{
    [TestClass]
    public class JsonOutputTest
    {
        /// <summary>
        /// Creates the objects, starts a listener on a new slot, runs the changes and returns the published messages.
        /// </summary>
        internal static async Task<List<PublishedMessage>> CaptureAsync(string name,
                                                                         string setupSql,
                                                                         string changesSql,
                                                                         int expectedCount,
                                                                         Func<PgOutput2JsonBuilder, PgOutput2JsonBuilder>? configure = null)
        {
            await Db.ExecuteAsync(setupSql);
            await Db.ExecuteAsync($"CREATE PUBLICATION {name}_pub FOR TABLE {name}");
            await Db.CreateSlotAsync($"{name}_slot");

            var publisher = new CollectingPublisherFactory();

            var builder = PgOutput2JsonBuilder.Create()
                .WithPgConnectionString(Containers.PgConnectionString)
                .WithPgPublications($"{name}_pub")
                .WithPgReplicationSlot($"{name}_slot")
                .WithMessagePublisherFactory(publisher);

            await using (new RunningListener(configure?.Invoke(builder) ?? builder))
            {
                await Db.ExecuteAsync(changesSql);

                await Wait.UntilAsync(() => publisher.Messages.Count >= expectedCount, $"{expectedCount} messages");
            }

            foreach (var message in publisher.Messages)
            {
                // whatever is published must be valid JSON
                using var json = JsonDocument.Parse(message.Json);
            }

            return publisher.Messages;
        }

        /// <summary>
        /// The JSON of the messages, with the position ("w") zeroed, since it differs from run to run.
        /// </summary>
        private static string[] Json(IEnumerable<PublishedMessage> messages)
        {
            return messages.Select(m => Regex.Replace(m.Json, "\"w\":\\d+", "\"w\":0")).ToArray();
        }

        private static string Row(PublishedMessage message)
        {
            using var json = JsonDocument.Parse(message.Json);

            return json.RootElement.GetProperty("r").GetRawText();
        }

        private static void AssertLines(string[] expected, string[] actual)
        {
            Assert.AreEqual(string.Join("\n", expected), string.Join("\n", actual));
        }

        [TestMethod]
        public async Task Default_mode_should_write_inserts_updates_key_changes_and_deletes()
        {
            var messages = await CaptureAsync("jo_basic",
                "CREATE TABLE jo_basic (id int PRIMARY KEY, v text, n int)",
                @"INSERT INTO jo_basic VALUES (1, 'a', NULL);
                  UPDATE jo_basic SET v = 'b' WHERE id = 1;
                  UPDATE jo_basic SET id = 2 WHERE id = 1;
                  DELETE FROM jo_basic WHERE id = 2;", 4);

            // the schema goes with the first row; a null is left out; the old key only when it changed
            AssertLines(
            [
                """{"c":"I","w":0,"n":1,"schema":{"tableName":"public.jo_basic","columns":[{"name":"id","isKey":true,"dataType":23},{"name":"v","isKey":false,"dataType":25},{"name":"n","isKey":false,"dataType":23}]},"r":{"id":1,"v":"a"}}""",
                """{"c":"U","w":0,"n":2,"r":{"id":1,"v":"b"}}""",
                """{"c":"U","w":0,"n":3,"k":{"id":1},"r":{"id":2,"v":"b"}}""",
                """{"c":"D","w":0,"n":4,"k":{"id":2}}""",
            ], Json(messages));

            CollectionAssert.AreEqual(new[] { "[1]", "[1]", "[2]", "[2]" }, messages.Select(m => m.Key).ToArray());
            Assert.IsTrue(messages.All(m => m.Table == "public.jo_basic"));
        }

        [TestMethod]
        public async Task Statements_of_one_transaction_should_share_the_position_and_count_up()
        {
            var messages = await CaptureAsync("jo_tx",
                "CREATE TABLE jo_tx (id int PRIMARY KEY)",
                @"BEGIN; INSERT INTO jo_tx VALUES (1), (2); INSERT INTO jo_tx VALUES (3); COMMIT;
                  INSERT INTO jo_tx VALUES (4);", 4);

            CollectionAssert.AreEqual(new ulong[] { 1, 2, 3, 1 }, messages.Select(m => m.MessageNo).ToArray());

            Assert.AreEqual(messages[0].TxFinalLsn, messages[2].TxFinalLsn);
            Assert.IsTrue(messages[3].TxFinalLsn > messages[2].TxFinalLsn);

            // the same key is in the JSON
            foreach (var message in messages)
            {
                Assert.IsTrue(message.Json.TryGetWalSeq(out var w, out var n));
                Assert.AreEqual((message.TxFinalLsn, message.MessageNo), (w, n));
            }
        }

        [TestMethod]
        public async Task Options_should_add_nulls_table_names_and_timestamps()
        {
            var before = DateTimeOffset.UtcNow.AddMinutes(-1);

            async Task<JsonElement> CaptureWithAsync(string name, TimestampFormat format)
            {
                var messages = await CaptureAsync(name,
                    $"CREATE TABLE {name} (id int PRIMARY KEY, v text)",
                    $"INSERT INTO {name} VALUES (1, NULL)", 1,
                    b => b.WithJsonOptions(o =>
                    {
                        o.WriteNulls = true;
                        o.WriteTableNames = true;
                        o.WriteTimestamps = true;
                        o.TimestampFormat = format;
                    }));

                using var json = JsonDocument.Parse(messages[0].Json);

                return json.RootElement.Clone();
            }

            var unix = await CaptureWithAsync("jo_opt_unix", TimestampFormat.UnixTimeMilliseconds);

            Assert.AreEqual("public.jo_opt_unix", unix.GetProperty("t").GetString());
            Assert.AreEqual("""{"id":1,"v":null}""", unix.GetProperty("r").GetRawText());

            foreach (var property in new[] { "cts", "mts" })
            {
                var time = DateTimeOffset.FromUnixTimeMilliseconds(unix.GetProperty(property).GetInt64());

                Assert.IsTrue(time > before && time < DateTimeOffset.UtcNow.AddMinutes(1), $"{property} is {time}");
            }

            var ticks = await CaptureWithAsync("jo_opt_ticks", TimestampFormat.DateTimeTicks);

            foreach (var property in new[] { "cts", "mts" })
            {
                var time = new DateTimeOffset(ticks.GetProperty(property).GetInt64(), TimeSpan.Zero);

                Assert.IsTrue(time > before && time < DateTimeOffset.UtcNow.AddMinutes(1), $"{property} is {time}");
            }
        }

        [TestMethod]
        public async Task Compact_mode_should_write_arrays_and_send_the_schema_when_it_is_new_or_changed()
        {
            var messages = await CaptureAsync("jo_compact",
                "CREATE TABLE jo_compact (id int PRIMARY KEY, v varchar(10), n numeric(10,2))",
                @"INSERT INTO jo_compact VALUES (1, 'a', NULL);
                  UPDATE jo_compact SET v = 'b' WHERE id = 1;
                  ALTER TABLE jo_compact ADD COLUMN extra bool;
                  INSERT INTO jo_compact VALUES (2, 'c', 1.5, true);
                  UPDATE jo_compact SET id = 3 WHERE id = 2;
                  DELETE FROM jo_compact WHERE id = 1;", 5,
                b => b.WithJsonOptions(o => o.WriteMode = JsonWriteMode.Compact));

            // schema: table, then name, is-key, type oid and the type modifier if there is one;
            // nulls are always written, to keep the positions
            AssertLines(
            [
                """{"c":"I","w":0,"n":1,"s":["public.jo_compact",["id",1,23],["v",0,1043,14],["n",0,1700,655366]],"r":[1,"a",null]}""",
                """{"c":"U","w":0,"n":2,"r":[1,"b",null]}""",
                """{"c":"I","w":0,"n":3,"s":["public.jo_compact",["id",1,23],["v",0,1043,14],["n",0,1700,655366],["extra",0,16]],"r":[2,"c",1.50,true]}""",
                """{"c":"U","w":0,"n":4,"k":[2],"r":[3,"c",1.50,true]}""",
                """{"c":"D","w":0,"n":5,"k":[1]}""",
            ], Json(messages));
        }

        [TestMethod]
        public async Task Schema_should_be_sent_again_for_each_table_after_a_reconnect()
        {
            await Db.ExecuteAsync(@"
                CREATE TABLE jo_re_a (id int PRIMARY KEY);
                CREATE TABLE jo_re_b (id int PRIMARY KEY);
                CREATE PUBLICATION jo_re_pub FOR TABLE jo_re_a, jo_re_b;");

            await Db.CreateSlotAsync("jo_re_slot");

            var publisher = new CollectingPublisherFactory();

            PgOutput2JsonBuilder CreateBuilder() => PgOutput2JsonBuilder.Create()
                .WithPgConnectionString(Containers.PgConnectionString)
                .WithPgPublications("jo_re_pub")
                .WithPgReplicationSlot("jo_re_slot")
                .WithJsonOptions(o => o.WriteMode = JsonWriteMode.Compact)
                .WithMessagePublisherFactory(publisher);

            await using (new RunningListener(CreateBuilder()))
            {
                await Db.ExecuteAsync("INSERT INTO jo_re_a VALUES (1); INSERT INTO jo_re_b VALUES (1); INSERT INTO jo_re_a VALUES (2); INSERT INTO jo_re_b VALUES (2);");

                await Wait.UntilAsync(() => publisher.Messages.Count >= 4, "4 messages");
                await Wait.UntilAsync(() => publisher.LastConfirmed.TxFinalLsn == publisher.Messages[3].TxFinalLsn, "the confirm");
            }

            await using (new RunningListener(CreateBuilder()))
            {
                await Db.ExecuteAsync("INSERT INTO jo_re_b VALUES (3); INSERT INTO jo_re_a VALUES (3);");

                await Wait.UntilAsync(() => publisher.Messages.Count >= 6, "6 messages");
            }

            var withSchema = publisher.Messages.Select(m => $"{m.Table}{m.Key}:{(m.Json.Contains("\"s\":") ? "schema" : "-")}").ToArray();

            AssertLines(
            [
                "public.jo_re_a[1]:schema", "public.jo_re_b[1]:schema", "public.jo_re_a[2]:-", "public.jo_re_b[2]:-",
                "public.jo_re_b[3]:schema", "public.jo_re_a[3]:schema",
            ], withSchema);
        }

        [TestMethod]
        public async Task Scalar_types_should_be_written_as_numbers_booleans_or_strings()
        {
            var messages = await CaptureAsync("jo_types",
                @"CREATE TABLE jo_types (id int PRIMARY KEY, i2 smallint, i8 bigint, num numeric, f4 real, f8 double precision, o oid,
                    b bool, by bytea, t text, vc varchar(20), ch char(3), u uuid, d date, ts timestamp, tstz timestamptz, tm time,
                    iv interval, j json, jb jsonb, pt point, ip inet, x xml, mny money)",
                @"INSERT INTO jo_types VALUES (1, -2, 9223372036854775807, 12345678901234567890.123456789, 1.5, 1e300, 42,
                    true, '\xDEADBEEF', E'quote"" back\\ nl\n tab\t unié ctl\u0001 emoji😀', 'vc', 'c', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
                    '2024-02-29', '2024-02-29 12:34:56.789', '2024-02-29 12:34:56.789+02', '12:34:56',
                    '1 day 02:03:04', '{""a"": [1, 2], ""b"": null}', '{""a"": [1, 2], ""b"": null}', '(1,2)', '10.0.0.1/24', '<a>1</a>', 12.34);
                  INSERT INTO jo_types (id, num, b) VALUES (2, 0.10, false);", 2);

            // numbers keep every digit; bytea is hex; json and jsonb are strings; timestamptz is in UTC
            Assert.AreEqual(
                """{"id":1,"i2":-2,"i8":9223372036854775807,"num":12345678901234567890.123456789,"f4":1.5,"f8":1e+300,"o":42,"b":true,"by":"deadbeef","t":"quote\" back\\ nl\n tab\t unié ctl\u0001 emoji😀","vc":"vc","ch":"c  ","u":"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11","d":"2024-02-29","ts":"2024-02-29 12:34:56.789","tstz":"2024-02-29 10:34:56.789+00","tm":"12:34:56","iv":"1 day 02:03:04","j":"{\"a\": [1, 2], \"b\": null}","jb":"{\"a\": [1, 2], \"b\": null}","pt":"(1,2)","ip":"10.0.0.1/24","x":"<a>1</a>","mny":"$12.34"}""",
                Row(messages[0]));

            Assert.AreEqual("""{"id":2,"num":0.10,"b":false}""", Row(messages[1]));
        }

        [TestMethod]
        public async Task Numbers_that_json_cannot_hold_should_be_written_as_null()
        {
            var messages = await CaptureAsync("jo_nan",
                "CREATE TABLE jo_nan (id int PRIMARY KEY, num numeric, f4 real, f8 double precision, a float8[])",
                "INSERT INTO jo_nan VALUES (1, 'NaN', 'Infinity', '-Infinity', '{1.5,NaN,Infinity}')", 1,
                b => b.WithJsonOptions(o => o.WriteNulls = true));

            Assert.AreEqual("""{"id":1,"num":null,"f4":null,"f8":null,"a":[1.5,null,null]}""", Row(messages[0]));
        }

        [TestMethod]
        public async Task Arrays_should_be_written_as_json_arrays()
        {
            var messages = await CaptureAsync("jo_arr",
                @"CREATE TABLE jo_arr (id int PRIMARY KEY, ai int[], an numeric[], af float8[], ab bool[], avc varchar[], ach char(2)[],
                    au uuid[], aj json[], ajb jsonb[], aby bytea[])",
                @"INSERT INTO jo_arr VALUES (1, '{1,NULL,3}', '{1.50,2}', '{0.5}', '{t,f,NULL}',
                    ARRAY['a', 'b,c', 'd""e', NULL, 'NULL', '', 'f\g', '{x}', 'h i']::varchar[], ARRAY['ab']::char(2)[],
                    ARRAY['a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', NULL]::uuid[], ARRAY['{""a"":1}']::json[], ARRAY['{""a"":1}', '[1,2]']::jsonb[],
                    ARRAY['\x01', NULL, '\xff']::bytea[]);
                  INSERT INTO jo_arr (id, ai, avc, aby) VALUES (2, '{{1,2},{3,4}}', '{{a,b},{c,d}}', '{}');
                  INSERT INTO jo_arr (id, ai, avc) VALUES (3, '{}', '{}');", 3);

            Assert.AreEqual(
                """{"id":1,"ai":[1,null,3],"an":[1.50,2],"af":[0.5],"ab":[true,false,null],"avc":["a","b,c","d\"e",null,"NULL","","f\\g","{x}","h i"],"ach":["ab"],"au":["a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",null],"aj":["{\"a\":1}"],"ajb":["{\"a\": 1}","[1, 2]"],"aby":["01",null,"ff"]}""",
                Row(messages[0]));

            Assert.AreEqual("""{"id":2,"ai":[[1,2],[3,4]],"avc":[["a","b"],["c","d"]],"aby":[]}""", Row(messages[1]));
            Assert.AreEqual("""{"id":3,"ai":[],"avc":[]}""", Row(messages[2]));
        }

        [TestMethod]
        public async Task Arrays_of_types_written_as_text_should_be_arrays_of_strings()
        {
            var messages = await CaptureAsync("jo_txt",
                @"CREATE TABLE jo_txt (id int PRIMARY KEY, at text[], ach ""char""[], anm name[], ad date[], atm time[], atz timetz[],
                    ats timestamp[], atstz timestamptz[], aiv interval[], apt point[], aip inet[], acd cidr[], amn money[], abt bit(2)[], avb varbit[])",
                @"INSERT INTO jo_txt VALUES (1, ARRAY['a', 'b c', NULL, ''], ARRAY['x']::""char""[], ARRAY['n']::name[], ARRAY['2024-02-29']::date[],
                    ARRAY['12:34:56']::time[], ARRAY['12:34:56+02']::timetz[], ARRAY['2024-02-29 12:34:56', NULL]::timestamp[],
                    ARRAY['2024-02-29 12:34:56+02']::timestamptz[], ARRAY['1 day']::interval[], ARRAY['(1,2)']::point[], ARRAY['10.0.0.1']::inet[],
                    ARRAY['10.0.0.0/8']::cidr[], ARRAY[12.34]::money[], ARRAY['10']::bit(2)[], ARRAY['101']::varbit[])", 1);

            Assert.AreEqual(
                """{"id":1,"at":["a","b c",null,""],"ach":["x"],"anm":["n"],"ad":["2024-02-29"],"atm":["12:34:56"],"atz":["12:34:56+02"],"ats":["2024-02-29 12:34:56",null],"atstz":["2024-02-29 10:34:56+00"],"aiv":["1 day"],"apt":["(1,2)"],"aip":["10.0.0.1"],"acd":["10.0.0.0/8"],"amn":["$12.34"],"abt":["10"],"avb":["101"]}""",
                Row(messages[0]));
        }

        [TestMethod]
        public async Task Array_of_a_type_the_library_does_not_know_should_be_written_as_the_postgres_literal()
        {
            var messages = await CaptureAsync("jo_lit",
                "CREATE TYPE jo_lit_mood AS ENUM ('sad', 'happy'); CREATE TABLE jo_lit (id int PRIMARY KEY, moods jo_lit_mood[], mood jo_lit_mood)",
                "INSERT INTO jo_lit VALUES (1, ARRAY['sad', 'happy']::jo_lit_mood[], 'happy')", 1);

            // the stream carries only the type oid, and the oid of a user type says nothing about it
            Assert.AreEqual("""{"id":1,"moods":"{sad,happy}","mood":"happy"}""", Row(messages[0]));
        }

        [TestMethod]
        public async Task Replica_identity_full_should_send_the_old_row_and_use_all_columns_as_the_key()
        {
            var messages = await CaptureAsync("jo_full",
                "CREATE TABLE jo_full (id int PRIMARY KEY, v text, n int); ALTER TABLE jo_full REPLICA IDENTITY FULL",
                "INSERT INTO jo_full VALUES (1, 'a', NULL); UPDATE jo_full SET v = 'b' WHERE id = 1; DELETE FROM jo_full WHERE id = 1;", 3);

            AssertLines(
            [
                """{"c":"I","w":0,"n":1,"schema":{"tableName":"public.jo_full","columns":[{"name":"id","isKey":true,"dataType":23},{"name":"v","isKey":true,"dataType":25},{"name":"n","isKey":true,"dataType":23}]},"r":{"id":1,"v":"a"}}""",
                """{"c":"U","w":0,"n":2,"k":{"id":1,"v":"a"},"r":{"id":1,"v":"b"}}""",
                """{"c":"D","w":0,"n":3,"k":{"id":1,"v":"b"}}""",
            ], Json(messages));

            // the key changes with the row
            CollectionAssert.AreEqual(new[] { "[1,\"a\"]", "[1,\"b\"]", "[1,\"b\"]" }, messages.Select(m => m.Key).ToArray());
        }

        [TestMethod]
        public async Task Replica_identity_using_index_should_use_the_index_columns_as_the_key()
        {
            var messages = await CaptureAsync("jo_index",
                @"CREATE TABLE jo_index (id int PRIMARY KEY, code text NOT NULL, v text);
                  CREATE UNIQUE INDEX jo_index_code ON jo_index (code);
                  ALTER TABLE jo_index REPLICA IDENTITY USING INDEX jo_index_code",
                @"INSERT INTO jo_index VALUES (1, 'x', 'a');
                  UPDATE jo_index SET v = 'b' WHERE id = 1;
                  UPDATE jo_index SET code = 'y' WHERE id = 1;
                  DELETE FROM jo_index WHERE id = 1;", 4);

            AssertLines(
            [
                """{"c":"I","w":0,"n":1,"schema":{"tableName":"public.jo_index","columns":[{"name":"id","isKey":false,"dataType":23},{"name":"code","isKey":true,"dataType":25},{"name":"v","isKey":false,"dataType":25}]},"r":{"id":1,"code":"x","v":"a"}}""",
                """{"c":"U","w":0,"n":2,"r":{"id":1,"code":"x","v":"b"}}""",
                """{"c":"U","w":0,"n":3,"k":{"code":"x"},"r":{"id":1,"code":"y","v":"b"}}""",
                """{"c":"D","w":0,"n":4,"k":{"code":"y"}}""",
            ], Json(messages));

            CollectionAssert.AreEqual(new[] { "[\"x\"]", "[\"x\"]", "[\"y\"]", "[\"y\"]" }, messages.Select(m => m.Key).ToArray());
        }

        [TestMethod]
        public async Task Table_without_a_replica_identity_should_have_an_empty_key()
        {
            var messages = await CaptureAsync("jo_nothing",
                "CREATE TABLE jo_nothing (id int, v text)",
                "INSERT INTO jo_nothing VALUES (1, 'a')", 1);

            Assert.AreEqual("", messages[0].Key);
            Assert.AreEqual("""{"id":1,"v":"a"}""", Row(messages[0]));
        }

        [TestMethod]
        public async Task Key_of_several_columns_should_be_an_array_in_column_order()
        {
            var messages = await CaptureAsync("jo_multi",
                "CREATE TABLE jo_multi (a int, v text, b text, c bool, PRIMARY KEY (c, a, b))",
                @"INSERT INTO jo_multi VALUES (1, 'v', 'x""y', true); UPDATE jo_multi SET v = 'w'; DELETE FROM jo_multi;", 3);

            CollectionAssert.AreEqual(Enumerable.Repeat("""[1,"x\"y",true]""", 3).ToArray(), messages.Select(m => m.Key).ToArray());

            Assert.AreEqual("""{"c":"D","w":0,"n":3,"k":{"a":1,"b":"x\"y","c":true}}""", Json(messages)[2]);
        }

        [TestMethod]
        public async Task Unchanged_toasted_value_should_be_marked()
        {
            foreach (var (name, mode) in new[] { ("jo_toast", JsonWriteMode.Default), ("jo_toast_c", JsonWriteMode.Compact) })
            {
                var messages = await CaptureAsync(name,
                    $"CREATE TABLE {name} (id int PRIMARY KEY, big text, v text); ALTER TABLE {name} ALTER COLUMN big SET STORAGE EXTERNAL",
                    $@"INSERT INTO {name} VALUES (1, repeat('x', 5000), 'a');
                       UPDATE {name} SET v = 'b' WHERE id = 1;
                       UPDATE {name} SET big = 'small' WHERE id = 1;", 3,
                    b => b.WithJsonOptions(o => o.WriteMode = mode));

                // PostgreSQL does not send a large value that the update did not touch
                Assert.AreEqual(5000, Regex.Match(Row(messages[0]), "x+").Length);

                if (mode == JsonWriteMode.Compact)
                {
                    Assert.AreEqual("""[1,"__TOAST__","b"]""", Row(messages[1]));
                    Assert.AreEqual("""[1,"small","b"]""", Row(messages[2]));
                }
                else
                {
                    Assert.AreEqual("""{"id":1,"big":"__TOAST__","v":"b"}""", Row(messages[1]));
                    Assert.AreEqual("""{"id":1,"big":"small","v":"b"}""", Row(messages[2]));
                }
            }
        }

        [TestMethod]
        public async Task Included_columns_should_limit_the_schema_the_row_and_the_key()
        {
            var messages = await CaptureAsync("jo_cols",
                "CREATE TABLE jo_cols (id int PRIMARY KEY, v text, secret text)",
                "INSERT INTO jo_cols VALUES (1, 'a', 's'); UPDATE jo_cols SET id = 2; DELETE FROM jo_cols;", 3,
                b => b.WithPgColumns("public.jo_cols", "id", "v"));

            AssertLines(
            [
                """{"c":"I","w":0,"n":1,"schema":{"tableName":"public.jo_cols","columns":[{"name":"id","isKey":true,"dataType":23},{"name":"v","isKey":false,"dataType":25}]},"r":{"id":1,"v":"a"}}""",
                """{"c":"U","w":0,"n":2,"k":{"id":1},"r":{"id":2,"v":"a"}}""",
                """{"c":"D","w":0,"n":3,"k":{"id":2}}""",
            ], Json(messages));

            // a key column that is not included leaves the key empty
            var withoutKey = await CaptureAsync("jo_cols_c",
                "CREATE TABLE jo_cols_c (id int PRIMARY KEY, v text, secret text)",
                "INSERT INTO jo_cols_c VALUES (1, 'a', 's'); DELETE FROM jo_cols_c;", 2,
                b => b.WithPgColumns("public.jo_cols_c", "v").WithJsonOptions(o => o.WriteMode = JsonWriteMode.Compact));

            AssertLines(
            [
                """{"c":"I","w":0,"n":1,"s":["public.jo_cols_c",["v",0,25]],"r":["a"]}""",
                """{"c":"D","w":0,"n":2,"k":[]}""",
            ], Json(withoutKey));

            CollectionAssert.AreEqual(new[] { "", "" }, withoutKey.Select(m => m.Key).ToArray());
        }
    }
}
