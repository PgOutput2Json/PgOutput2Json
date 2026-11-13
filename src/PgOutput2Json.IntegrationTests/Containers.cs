using System;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;
using Testcontainers.DynamoDb;
using Testcontainers.Kafka;
using Testcontainers.MongoDb;
using Testcontainers.PostgreSql;

namespace PgOutput2Json.IntegrationTests
{
    /// <summary>
    /// One PostgreSQL container for the whole test run. The outputs are started only when a test asks for them.
    /// </summary>
    [TestClass]
    public static class Containers
    {
        private static PostgreSqlContainer? _postgres;
        private static KafkaContainer? _kafka;
        private static MongoDbContainer? _mongo;
        private static DynamoDbContainer? _dynamoDb;

        public static string PgConnectionString => _postgres!.GetConnectionString();

        /// <summary>
        /// The PostgreSQL image to test against, e.g. PG_IMAGE=postgres:14. The CI runs the tests for each supported version.
        /// </summary>
        public static string PgImage => Environment.GetEnvironmentVariable("PG_IMAGE") is { Length: > 0 } image ? image : "postgres:16";

        public static int PgMajorVersion { get; private set; }

        /// <summary>
        /// Ends the test as inconclusive on a PostgreSQL version without the feature it needs.
        /// </summary>
        public static void RequirePg(int majorVersion, string feature)
        {
            if (PgMajorVersion < majorVersion) Assert.Inconclusive($"{feature} need PostgreSQL {majorVersion}, this is {PgMajorVersion}");
        }

        [AssemblyInitialize]
        public static async Task StartAsync(TestContext _)
        {
            // the AWS clients of the DynamoDB and Kinesis outputs take their credentials from the environment;
            // the local emulators accept anything
            Environment.SetEnvironmentVariable("AWS_ACCESS_KEY_ID", Environment.GetEnvironmentVariable("AWS_ACCESS_KEY_ID") ?? "test");
            Environment.SetEnvironmentVariable("AWS_SECRET_ACCESS_KEY", Environment.GetEnvironmentVariable("AWS_SECRET_ACCESS_KEY") ?? "test");

            _postgres = new PostgreSqlBuilder(PgImage)
                .WithCommand("-c", "wal_level=logical", "-c", "max_replication_slots=100", "-c", "max_wal_senders=100")
                .Build();

            await _postgres.StartAsync();

            PgMajorVersion = await Db.ScalarAsync<int>("SELECT current_setting('server_version_num')::int / 10000");
        }

        public static async Task<string> GetKafkaBootstrapServersAsync()
        {
            if (_kafka == null)
            {
                _kafka = new KafkaBuilder("apache/kafka:3.9.0").Build();

                await _kafka.StartAsync();
            }

            return _kafka.GetBootstrapAddress();
        }

        public static async Task<string> GetMongoConnectionStringAsync()
        {
            if (_mongo == null)
            {
                _mongo = new MongoDbBuilder("mongo:8").Build();

                await _mongo.StartAsync();
            }

            return _mongo.GetConnectionString();
        }

        public static async Task<string> GetDynamoDbUrlAsync()
        {
            if (_dynamoDb == null)
            {
                _dynamoDb = new DynamoDbBuilder("amazon/dynamodb-local:latest").Build();

                await _dynamoDb.StartAsync();
            }

            return _dynamoDb.GetConnectionString();
        }

        [AssemblyCleanup]
        public static async Task StopAsync()
        {
            if (_kafka != null) await _kafka.DisposeAsync();
            if (_mongo != null) await _mongo.DisposeAsync();
            if (_dynamoDb != null) await _dynamoDb.DisposeAsync();
            if (_postgres != null) await _postgres.DisposeAsync();
        }
    }
}
