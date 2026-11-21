using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Driver;

namespace PgOutput2Json.IntegrationTests.Outputs
{
    [TestClass]
    public class MongoDbStoreTest
    {
        [TestMethod]
        public async Task MongoDb_should_mirror_the_tables()
        {
            var settings = MongoClientSettings.FromConnectionString(await Containers.GetMongoConnectionStringAsync());

            using var client = new MongoClient(settings);

            var database = client.GetDatabase("os_mongo_db");

            // the documents of the collection of a table, without the _id, ordered by id, with the fields in name order
            async Task<string> ReadAsync(string table)
            {
                var documents = await database.GetCollection<BsonDocument>($"public__{table}")
                    .Find(FilterDefinition<BsonDocument>.Empty)
                    .Project(Builders<BsonDocument>.Projection.Exclude("_id"))
                    .Sort(Builders<BsonDocument>.Sort.Ascending("id"))
                    .ToListAsync();

                // the order of the fields depends on which change created the document
                documents = documents.Select(d => new BsonDocument(d.Elements.OrderBy(e => e.Name, System.StringComparer.Ordinal))).ToList();

                foreach (var document in documents)
                {
                    if (document.TryGetValue("big", out var big) && big.IsString && big.AsString.Length > 100)
                    {
                        document["big"] = $"{big.AsString[0]}*{big.AsString.Length}";
                    }
                }

                return string.Join("\n", documents.Select(d => d.ToJson(new JsonWriterSettings { OutputMode = JsonOutputMode.RelaxedExtendedJson })));
            }

            await StoreScenario.RunAsync("os_mongo",
                builder => builder.UseMongoDb(options =>
                {
                    options.ClientSettings = settings;
                    options.DatabaseName = "os_mongo_db";
                }),
                ReadAsync,
                expectedAfterCopy:
                    """
                    { "b" : true, "big" : "x*3000", "d" : { "$binary" : { "base64" : "AQI=", "subType" : "00" } }, "id" : 1, "n" : 1.5, "tags" : ["a", "b c"], "v" : "one" }
                    { "b" : false, "big" : null, "d" : null, "id" : 2, "n" : null, "tags" : null, "v" : "two" }
                    """,
                expected:
                    """
                    { "b" : true, "big" : "x*3000", "d" : { "$binary" : { "base64" : "AQI=", "subType" : "00" } }, "id" : 1, "n" : 1.5, "tags" : ["a", "b c"], "v" : "uno" }
                    { "b" : false, "big" : null, "d" : null, "id" : 2, "n" : 2.25, "tags" : ["x"], "v" : null }
                    { "b" : null, "big" : null, "d" : { "$binary" : { "base64" : "/w==", "subType" : "00" } }, "id" : 30, "n" : 3.0, "tags" : [], "v" : "three" }
                    """,
                expectedKeyLast:
                    """
                    { "id" : 1, "v" : "changed" }
                    { "id" : 2, "v" : "new" }
                    """,
                expectedCompositeKey:
                    """
                    { "code" : "x", "id" : 1, "v" : "changed" }
                    { "code" : "zz", "id" : 3, "v" : "new" }
                    """);
        }

        [TestMethod]
        public async Task Arrays_of_enums_and_domains_should_be_arrays_of_strings_in_the_copy_and_the_stream()
        {
            var settings = MongoClientSettings.FromConnectionString(await Containers.GetMongoConnectionStringAsync());

            using var client = new MongoClient(settings);

            var database = client.GetDatabase("os_mongo_arr_db");

            // row 1 is copied, row 2 streamed; an enum is a string, and an array of it (also of two dimensions) an array of strings
            await Db.ExecuteAsync(@"
                CREATE TYPE os_mongo_arr_mood AS ENUM ('sad', 'so so');
                CREATE DOMAIN os_mongo_arr_ints AS int[];
                CREATE TABLE os_mongo_arr (id int PRIMARY KEY, mood os_mongo_arr_mood, moods os_mongo_arr_mood[], grid os_mongo_arr_mood[], ints os_mongo_arr_ints);
                INSERT INTO os_mongo_arr VALUES (1, 'sad', '{sad,""so so"",NULL}', '{{sad,sad},{""so so"",NULL}}', '{1,2}');
                CREATE PUBLICATION os_mongo_arr_pub FOR TABLE os_mongo_arr;");

            await Db.CreateSlotAsync("os_mongo_arr_slot");

            var builder = PgOutput2JsonBuilder.Create()
                .WithPgConnectionString(Containers.PgConnectionString)
                .WithPgPublications("os_mongo_arr_pub")
                .WithPgReplicationSlot("os_mongo_arr_slot")
                .WithInitialDataCopy(true)
                .UseMongoDb(options =>
                {
                    options.ClientSettings = settings;
                    options.DatabaseName = "os_mongo_arr_db";
                });

            var expected =
                """
                { "grid" : [["sad", "sad"], ["so so", null]], "id" : 1, "ints" : ["1", "2"], "mood" : "sad", "moods" : ["sad", "so so", null] }
                { "grid" : null, "id" : 2, "ints" : [], "mood" : null, "moods" : ["so so"] }
                """.ReplaceLineEndings("\n");

            var actual = "";

            await using (new RunningListener(builder))
            {
                await Wait.UntilAsync(async () => await Db.TableStatesAsync("os_mongo_arr_slot") == "public.os_mongo_arr:ready", "the table ready");

                await Db.ExecuteAsync("INSERT INTO os_mongo_arr VALUES (2, NULL, '{\"so so\"}', NULL, '{}')");

                try
                {
                    await Wait.UntilAsync(async () =>
                    {
                        var documents = await database.GetCollection<BsonDocument>("public__os_mongo_arr")
                            .Find(FilterDefinition<BsonDocument>.Empty)
                            .Project(Builders<BsonDocument>.Projection.Exclude("_id"))
                            .Sort(Builders<BsonDocument>.Sort.Ascending("id"))
                            .ToListAsync();

                        actual = string.Join("\n", documents
                            .Select(d => new BsonDocument(d.Elements.OrderBy(e => e.Name, System.StringComparer.Ordinal)))
                            .Select(d => d.ToJson(new JsonWriterSettings { OutputMode = JsonOutputMode.RelaxedExtendedJson })));

                        return actual == expected;
                    }, "the rows in MongoDB");
                }
                catch (System.TimeoutException)
                {
                    Assert.AreEqual(expected, actual);
                }
            }
        }

        /// <summary>
        /// A timestamp without time zone is stored as its wall time in UTC, from the initial copy and from the stream, so
        /// it reads back as the same wall time on any machine (the CI runs this with TZ=Europe/Zagreb). A null element of
        /// an array stays null, and a value a BSON date cannot hold (infinity, a date BC) is stored as PostgreSQL's text.
        /// </summary>
        [TestMethod]
        public async Task MongoDb_should_store_timestamps_without_the_local_offset()
        {
            var settings = MongoClientSettings.FromConnectionString(await Containers.GetMongoConnectionStringAsync());

            using var client = new MongoClient(settings);

            var database = client.GetDatabase("os_mongo_ts_db");

            await Db.ExecuteAsync(@"
                CREATE TABLE os_mongo_ts (id int PRIMARY KEY, ts timestamp, tstz timestamptz, tsa timestamp[]);
                INSERT INTO os_mongo_ts VALUES (1, '2026-01-02 03:04:05.123456', '2026-01-02 03:04:05.123456+01', '{""2026-06-30 23:59:59"",NULL}');
                CREATE PUBLICATION os_mongo_ts_pub FOR TABLE os_mongo_ts;");

            await Db.CreateSlotAsync("os_mongo_ts_slot");

            var builder = PgOutput2JsonBuilder.Create()
                .WithPgConnectionString(Containers.PgConnectionString)
                .WithPgPublications("os_mongo_ts_pub")
                .WithPgReplicationSlot("os_mongo_ts_slot")
                .WithInitialDataCopy(true)
                .UseMongoDb(options =>
                {
                    options.ClientSettings = settings;
                    options.DatabaseName = "os_mongo_ts_db";
                });

            const string copied =
                """{ "id" : 1, "ts" : { "$date" : "2026-01-02T03:04:05.123Z" }, "tsa" : [{ "$date" : "2026-06-30T23:59:59Z" }, null], "tstz" : { "$date" : "2026-01-02T02:04:05.123Z" } }""";

            const string streamed =
                """{ "id" : 2, "ts" : "infinity", "tsa" : ["-infinity", "0044-03-15 12:00:00 BC"], "tstz" : { "$date" : "2026-06-30T21:59:59Z" } }""";

            var actual = "";

            async Task<bool> StoredAsync(string expected)
            {
                var documents = await database.GetCollection<BsonDocument>("public__os_mongo_ts")
                    .Find(FilterDefinition<BsonDocument>.Empty)
                    .Project(Builders<BsonDocument>.Projection.Exclude("_id"))
                    .Sort(Builders<BsonDocument>.Sort.Ascending("id"))
                    .ToListAsync();

                actual = string.Join("\n", documents
                    .Select(d => new BsonDocument(d.Elements.OrderBy(e => e.Name, System.StringComparer.Ordinal)))
                    .Select(d => d.ToJson(new JsonWriterSettings { OutputMode = JsonOutputMode.RelaxedExtendedJson })));

                return actual == expected;
            }

            await using (new RunningListener(builder))
            {
                try
                {
                    await Wait.UntilAsync(() => StoredAsync(copied), "the copied row");

                    await Db.ExecuteAsync("INSERT INTO os_mongo_ts VALUES (2, 'infinity', '2026-06-30 23:59:59+02', '{-infinity,\"0044-03-15 12:00:00 BC\"}')");

                    await Wait.UntilAsync(() => StoredAsync(copied + "\n" + streamed), "the streamed row");
                }
                catch (System.TimeoutException e)
                {
                    Assert.Fail($"{e.Message}. Stored: {actual}");
                }
            }
        }

        [TestMethod]
        public async Task Array_in_the_key_should_be_its_JSON_text_so_rows_can_share_elements()
        {
            var settings = MongoClientSettings.FromConnectionString(await Containers.GetMongoConnectionStringAsync());

            using var client = new MongoClient(settings);

            var database = client.GetDatabase("os_mongo_akey_db");

            // the keys share the element "a": as arrays, the unique index would reject the second row
            await Db.ExecuteAsync(@"
                CREATE TYPE os_mongo_akey_mood AS ENUM ('sad', 'so so');
                CREATE TABLE os_mongo_akey (id text[] PRIMARY KEY, v text, tags text[]);
                CREATE TABLE os_mongo_akey_enum (id os_mongo_akey_mood[] PRIMARY KEY, v text);
                INSERT INTO os_mongo_akey VALUES ('{a,b}', 'copied', '{x}');
                INSERT INTO os_mongo_akey_enum VALUES ('{sad,""so so""}', 'copied');
                CREATE PUBLICATION os_mongo_akey_pub FOR TABLE os_mongo_akey, os_mongo_akey_enum;");

            await Db.CreateSlotAsync("os_mongo_akey_slot");

            using var logs = new CollectingLoggerFactory();

            var builder = PgOutput2JsonBuilder.Create()
                .WithLoggerFactory(logs)
                .WithPgConnectionString(Containers.PgConnectionString)
                .WithPgPublications("os_mongo_akey_pub")
                .WithPgReplicationSlot("os_mongo_akey_slot")
                .WithInitialDataCopy(true)
                .UseMongoDb(options =>
                {
                    options.ClientSettings = settings;
                    options.DatabaseName = "os_mongo_akey_db";
                });

            // the other array columns stay arrays; an array of an enum in the key is the JSON text too
            var expected =
                """
                { "id" : "[\"a\",\"b\"]", "tags" : ["x"], "v" : "updated" }
                { "id" : "[\"a\",\"d\"]", "tags" : ["y", "z"], "v" : "key changed" }
                { "id" : "[\"sad\",\"so so\"]", "v" : "copied" }
                { "id" : "[\"sad\"]", "v" : "streamed" }
                """.ReplaceLineEndings("\n");

            var actual = "";

            await using (new RunningListener(builder))
            {
                await Wait.UntilAsync(async () => await Db.TableStatesAsync("os_mongo_akey_slot") == "public.os_mongo_akey:ready,public.os_mongo_akey_enum:ready", "the tables ready");

                await Db.ExecuteAsync(@"
                    INSERT INTO os_mongo_akey VALUES ('{a,c}', 'streamed', '{y,z}'), ('{e}', 'deleted', NULL);
                    UPDATE os_mongo_akey SET v = 'updated' WHERE id = '{a,b}';
                    UPDATE os_mongo_akey SET id = '{a,d}', v = 'key changed' WHERE id = '{a,c}';
                    DELETE FROM os_mongo_akey WHERE id = '{e}';
                    INSERT INTO os_mongo_akey_enum VALUES ('{sad}', 'streamed');");

                try
                {
                    await Wait.UntilAsync(async () =>
                    {
                        var documents = new List<BsonDocument>();

                        foreach (var collection in new[] { "public__os_mongo_akey", "public__os_mongo_akey_enum" })
                        {
                            documents.AddRange(await database.GetCollection<BsonDocument>(collection)
                                .Find(FilterDefinition<BsonDocument>.Empty)
                                .Project(Builders<BsonDocument>.Projection.Exclude("_id"))
                                .Sort(Builders<BsonDocument>.Sort.Ascending("id"))
                                .ToListAsync());
                        }

                        actual = string.Join("\n", documents
                            .Select(d => new BsonDocument(d.Elements.OrderBy(e => e.Name, System.StringComparer.Ordinal)))
                            .Select(d => d.ToJson(new JsonWriterSettings { OutputMode = JsonOutputMode.RelaxedExtendedJson })));

                        return actual == expected;
                    }, "the rows in MongoDB");
                }
                catch (System.TimeoutException)
                {
                    Assert.AreEqual(expected, actual, $"Logged: {logs}");
                }
            }
        }

        [TestMethod]
        public async Task Position_saved_in_two_values_should_be_read_and_saved_again_as_one()
        {
            var settings = MongoClientSettings.FromConnectionString(await Containers.GetMongoConnectionStringAsync());

            using var client = new MongoClient(settings);

            var config = client.GetDatabase("os_mongo_pos_db").GetCollection<BsonDocument>("__pg2j_config");

            // a store of an earlier version: the two parts of the position under keys of their own
            await config.InsertManyAsync([new BsonDocument { ["_id"] = "WalEnd", ["Value"] = "200" }, new BsonDocument { ["_id"] = "MessageNo", ["Value"] = "5" }]);

            var options = new global::PgOutput2Json.MongoDb.MongoDbPublisherOptions { ClientSettings = settings, DatabaseName = "os_mongo_pos_db" };

            await using (var publisher = new global::PgOutput2Json.MongoDb.MongoDbPublisher(options, null))
            {
                Assert.AreEqual((200UL, 5UL), await publisher.GetLastPublishedWalSeqAsync(default));

                await publisher.PublishAsync(StoreMessage.Create(300, 1), default);
                await publisher.ConfirmAsync(default);
            }

            var position = await config.Find(Builders<BsonDocument>.Filter.Eq("_id", "Position")).FirstAsync();

            Assert.AreEqual("300/1", position["Value"].AsString);

            await using (var publisher = new global::PgOutput2Json.MongoDb.MongoDbPublisher(options, null))
            {
                Assert.AreEqual((300UL, 1UL), await publisher.GetLastPublishedWalSeqAsync(default));
            }
        }

        private static string Number(BsonValue value)
        {
            return value.IsDouble ? value.AsDouble.ToString("R", System.Globalization.CultureInfo.InvariantCulture) : value.ToString()!;
        }

        [TestMethod]
        public async Task Json_values_that_are_not_objects_and_numbers_should_be_stored_as_they_are()
        {
            var settings = MongoClientSettings.FromConnectionString(await Containers.GetMongoConnectionStringAsync());

            using var client = new MongoClient(settings);

            var database = client.GetDatabase("os_mongo_json_db");

            await Db.ExecuteAsync(@"
                CREATE TABLE os_mongo_json (id int PRIMARY KEY, j json, jb jsonb, ja jsonb[], n numeric, f float8);
                CREATE PUBLICATION os_mongo_json_pub FOR TABLE os_mongo_json;");

            await Db.CreateSlotAsync("os_mongo_json_slot");

            using var logs = new CollectingLoggerFactory();

            var builder = PgOutput2JsonBuilder.Create()
                .WithLoggerFactory(logs)
                .WithPgConnectionString(Containers.PgConnectionString)
                .WithPgPublications("os_mongo_json_pub")
                .WithPgReplicationSlot("os_mongo_json_slot")
                .UseMongoDb(options =>
                {
                    options.ClientSettings = settings;
                    options.DatabaseName = "os_mongo_json_db";
                });

            var expected =
                """
                { "id" : 1, "j" : { "a" : 2, "b" : { "c" : [1, "x"] } }, "ja" : [{ "a" : 1 }, [1, 2], 5, "x", null], "jb" : { "a" : 1 }, "numbers" : "0.1 0.1" }
                { "id" : 2, "j" : [1, 2], "ja" : null, "jb" : [{ "a" : 1 }], "numbers" : "12345.678901 12345.678901" }
                { "id" : 3, "j" : 5, "ja" : null, "jb" : "text", "numbers" : "BsonNull BsonNull" }
                { "id" : 4, "j" : null, "ja" : null, "jb" : true, "numbers" : "BsonNull BsonNull" }
                { "id" : 5, "j" : null, "ja" : null, "jb" : null, "numbers" : "BsonNull BsonNull" }
                """.ReplaceLineEndings("\n");

            var actual = "";

            await using (new RunningListener(builder))
            {
                // row 1 has a key twice in the json; row 4 has a JSON null, row 5 an SQL NULL
                await Db.ExecuteAsync(@"
                    INSERT INTO os_mongo_json VALUES
                        (1, '{""a"": 1, ""a"": 2, ""b"": {""c"": [1, ""x""]}}', '{""a"": 1}', ARRAY['{""a"": 1}', '[1, 2]', '5', '""x""', NULL]::jsonb[], 0.1, 0.1),
                        (2, '[1, 2]', '[{""a"": 1}]', NULL, 12345.678901, 12345.678901),
                        (3, '5', '""text""', NULL, NULL, NULL),
                        (4, 'null', 'true', NULL, NULL, NULL),
                        (5, NULL, NULL, NULL, NULL, NULL);");

                try
                {
                    await Wait.UntilAsync(async () =>
                    {
                        var documents = await database.GetCollection<BsonDocument>("public__os_mongo_json")
                            .Find(FilterDefinition<BsonDocument>.Empty)
                            .Project(Builders<BsonDocument>.Projection.Exclude("_id"))
                            .Sort(Builders<BsonDocument>.Sort.Ascending("id"))
                            .ToListAsync();

                        // the numbers are compared as the shortest text that gives the same double, which the JSON of the driver is not
                        foreach (var document in documents)
                        {
                            document["numbers"] = $"{Number(document["n"])} {Number(document["f"])}";

                            document.Remove("n");
                            document.Remove("f");
                        }

                        actual = string.Join("\n", documents
                            .Select(d => new BsonDocument(d.Elements.OrderBy(e => e.Name, System.StringComparer.Ordinal)))
                            .Select(d => d.ToJson(new JsonWriterSettings { OutputMode = JsonOutputMode.RelaxedExtendedJson })));

                        return actual == expected;
                    }, "the rows in MongoDB");
                }
                catch (System.TimeoutException)
                {
                    Assert.AreEqual(expected, actual, $"Logged: {logs}");
                }
            }
        }
    }
}
