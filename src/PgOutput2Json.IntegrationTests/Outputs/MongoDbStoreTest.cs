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
    }
}
