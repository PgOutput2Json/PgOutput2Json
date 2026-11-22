using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Microsoft.Data.Sqlite;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MongoDB.Bson;
using MongoDB.Driver;

using PgOutput2Json.ClickHouse;
using PgOutput2Json.DynamoDb;
using PgOutput2Json.MongoDb;
using PgOutput2Json.Sqlite;

namespace PgOutput2Json.IntegrationTests.Outputs
{
    /// <summary>
    /// The name a store gives the table of a PostgreSQL table, from the schema and the table name apart (GetTableName):
    /// by default schema.table for MongoDB and DynamoDB, schema_table for SQLite and ClickHouse, and the table name alone
    /// for the public schema. A table name with a dot, which the joined name quotes, is not taken apart.
    /// </summary>
    [TestClass]
    public class StoreTableNameTest
    {
        /// <summary>
        /// Creates a schema with the table "a.b" and one row, and runs the listener with the copy until <paramref name="hasRowAsync"/>.
        /// </summary>
        private static async Task RunAsync(string schema, Func<PgOutput2JsonBuilder, PgOutput2JsonBuilder> useStore, Func<Task<bool>> hasRowAsync)
        {
            await Db.ExecuteAsync($@"
                CREATE SCHEMA {schema};
                CREATE TABLE {schema}.""a.b"" (id int PRIMARY KEY, v text);
                INSERT INTO {schema}.""a.b"" VALUES (1, 'one');
                CREATE PUBLICATION {schema}_pub FOR TABLE {schema}.""a.b"";");

            await Db.CreateSlotAsync($"{schema}_slot");

            var builder = useStore(PgOutput2JsonBuilder.Create()
                .WithPgConnectionString(Containers.PgConnectionString)
                .WithPgPublications($"{schema}_pub")
                .WithPgReplicationSlot($"{schema}_slot")
                .WithInitialDataCopy(true));

            await using (new RunningListener(builder))
            {
                await Wait.UntilAsync(hasRowAsync, "the row in the table of the store");
            }
        }

        private static string SqlitePath() => Path.Combine(Path.GetTempPath(), $"pg2j_{Guid.NewGuid():N}.db");

        private static async Task<string?> SqliteValueAsync(string path, string table)
        {
            await using var connection = new SqliteConnection($"Data Source={path};Pooling=False");
            await connection.OpenAsync();

            await using var command = connection.CreateCommand();
            command.CommandText = $"SELECT v FROM \"{table}\" WHERE id = 1";

            try
            {
                return (string?)await command.ExecuteScalarAsync();
            }
            catch (SqliteException)
            {
                return null;
            }
        }

        [TestMethod]
        public async Task Sqlite_should_name_a_table_schema_table_by_default()
        {
            var path = SqlitePath();

            try
            {
                await RunAsync("stn_sqlite",
                    b => b.UseSqlite(o => o.ConnectionStringBuilder = new SqliteConnectionStringBuilder($"Data Source={path};Pooling=False")),
                    async () => await SqliteValueAsync(path, "stn_sqlite_a.b") == "one");
            }
            finally
            {
                File.Delete(path);
            }
        }

        [TestMethod]
        public async Task Sqlite_should_take_the_name_of_GetTableName()
        {
            var path = SqlitePath();

            try
            {
                await RunAsync("stn_custom",
                    b => b.UseSqlite(o =>
                    {
                        o.ConnectionStringBuilder = new SqliteConnectionStringBuilder($"Data Source={path};Pooling=False");
                        o.GetTableName = (schema, table) => $"{schema}|{table}";
                    }),
                    async () => await SqliteValueAsync(path, "stn_custom|a.b") == "one");
            }
            finally
            {
                File.Delete(path);
            }
        }

        /// <summary>
        /// Two tables with the same name in SQLite would mix their rows: the output stops with an error that names both.
        /// </summary>
        [TestMethod]
        public async Task Sqlite_should_fail_when_two_tables_get_the_same_name()
        {
            var path = SqlitePath();

            await Db.ExecuteAsync(@"
                CREATE SCHEMA stn_twice;
                CREATE TABLE stn_twice.t (id int PRIMARY KEY);
                CREATE TABLE stn_twice.u (id int PRIMARY KEY);
                INSERT INTO stn_twice.t VALUES (1);
                INSERT INTO stn_twice.u VALUES (1);
                CREATE PUBLICATION stn_twice_pub FOR TABLE stn_twice.t, stn_twice.u;");

            await Db.CreateSlotAsync("stn_twice_slot");

            using var logs = new CollectingLoggerFactory();

            var builder = PgOutput2JsonBuilder.Create()
                .WithLoggerFactory(logs)
                .WithPgConnectionString(Containers.PgConnectionString)
                .WithPgPublications("stn_twice_pub")
                .WithPgReplicationSlot("stn_twice_slot")
                .WithInitialDataCopy(true)
                .UseSqlite(o =>
                {
                    o.ConnectionStringBuilder = new SqliteConnectionStringBuilder($"Data Source={path};Pooling=False");
                    o.GetTableName = (schema, table) => "same";
                });

            try
            {
                await using (new RunningListener(builder))
                {
                    await Wait.UntilAsync(() => logs.Contains(Microsoft.Extensions.Logging.LogLevel.Error, "both have the name same"), "the error");
                }

                Assert.IsTrue(logs.Contains(Microsoft.Extensions.Logging.LogLevel.Error, "stn_twice.t"), logs.ToString());
                Assert.IsTrue(logs.Contains(Microsoft.Extensions.Logging.LogLevel.Error, "stn_twice.u"), logs.ToString());
            }
            finally
            {
                File.Delete(path);
            }
        }

        [TestMethod]
        public async Task MongoDb_should_name_a_collection_schema_dot_table_by_default()
        {
            var settings = MongoClientSettings.FromConnectionString(await Containers.GetMongoConnectionStringAsync());

            using var client = new MongoClient(settings);

            var database = client.GetDatabase("stn_mongo_db");

            await RunAsync("stn_mongo",
                b => b.UseMongoDb(o =>
                {
                    o.ClientSettings = settings;
                    o.DatabaseName = "stn_mongo_db";
                }),
                async () => (await database.GetCollection<BsonDocument>("stn_mongo.a.b").Find(FilterDefinition<BsonDocument>.Empty).ToListAsync())
                    .Any(d => d["v"] == "one"));
        }

        [TestMethod]
        public async Task DynamoDb_should_name_a_table_schema_dot_table_by_default()
        {
            var config = new AmazonDynamoDBConfig { ServiceURL = await Containers.GetDynamoDbUrlAsync() };

            using var client = new AmazonDynamoDBClient(config);

            await RunAsync("stn_dynamo",
                b => b.UseDynamoDb(o => o.ClientConfig = config),
                async () =>
                {
                    try
                    {
                        var items = (await client.ScanAsync(new ScanRequest { TableName = "stn_dynamo.a.b", ConsistentRead = true })).Items;
                        return items.Any(i => i.TryGetValue("v", out var v) && v.S == "one");
                    }
                    catch (ResourceNotFoundException)
                    {
                        return false;
                    }
                });
        }

        [TestMethod]
        public async Task ClickHouse_should_name_a_table_schema_table_by_default()
        {
            var endpoint = await Containers.GetClickHouseEndpointAsync();

            await RunAsync("stn_click",
                b => b.UseClickHouse(o => ClickHouseStoreTest.Configure(o, endpoint)),
                async () => await ClickHouseStoreTest.QueryAsync(endpoint, "SELECT v FROM `stn_click_a.b` FINAL WHERE id = 1 FORMAT TSVRaw") == "one");
        }
    }
}
