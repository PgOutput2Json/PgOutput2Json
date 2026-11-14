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
