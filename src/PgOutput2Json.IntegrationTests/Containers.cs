using System;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;
using Testcontainers.DynamoDb;
using Testcontainers.EventHubs;
using Testcontainers.Kafka;
using Testcontainers.LocalStack;
using Testcontainers.MongoDb;
using Testcontainers.PostgreSql;
using Testcontainers.RabbitMq;
using Testcontainers.Redis;

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
        private static RabbitMqContainer? _rabbitMq;
        private static RedisContainer? _redis;
        private static LocalStackContainer? _localStack;
        private static EventHubsContainer? _eventHubs;

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

            await StartWithRetryAsync(_postgres);

            PgMajorVersion = await Db.ScalarAsync<int>("SELECT current_setting('server_version_num')::int / 10000");
        }

        public static async Task<string> GetKafkaBootstrapServersAsync()
        {
            if (_kafka == null)
            {
                _kafka = new KafkaBuilder("apache/kafka:3.9.0").Build();

                await StartWithRetryAsync(_kafka);
            }

            return _kafka.GetBootstrapAddress();
        }

        public static async Task<string> GetMongoConnectionStringAsync()
        {
            if (_mongo == null)
            {
                _mongo = new MongoDbBuilder("mongo:8").Build();

                await StartWithRetryAsync(_mongo);
            }

            return _mongo.GetConnectionString();
        }

        public static async Task<string> GetDynamoDbUrlAsync()
        {
            if (_dynamoDb == null)
            {
                _dynamoDb = new DynamoDbBuilder("amazon/dynamodb-local:latest").WithPortBinding(FreePort(), 8000).Build();

                await StartWithRetryAsync(_dynamoDb);
            }

            return _dynamoDb.GetConnectionString();
        }

        /// <summary>
        /// RabbitMQ with the stream plugin. The stream port is mapped to a random port, so stream clients
        /// must use an address resolver for <see cref="RabbitMqStreamPort"/> on localhost.
        /// </summary>
        public static async Task<RabbitMqContainer> GetRabbitMqAsync()
        {
            if (_rabbitMq == null)
            {
                var container = new RabbitMqBuilder("rabbitmq:4-management")
                    .WithPortBinding(5552, true)
                    .WithEnvironment("RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS", "-rabbitmq_stream advertised_host localhost")
                    .Build();

                await StartWithRetryAsync(container);

                var result = await container.ExecAsync(["rabbitmq-plugins", "enable", "rabbitmq_stream"]);

                if (result.ExitCode != 0) throw new InvalidOperationException($"Could not enable the stream plugin: {result.Stderr}");

                _rabbitMq = container;
            }

            return _rabbitMq;
        }

        public static int RabbitMqStreamPort => _rabbitMq!.GetMappedPublicPort(5552);

        public static async Task<string> GetRedisConnectionStringAsync()
        {
            if (_redis == null)
            {
                _redis = new RedisBuilder("redis:7").WithPortBinding(FreePort(), 6379).Build();

                await StartWithRetryAsync(_redis);
            }

            return _redis.GetConnectionString();
        }

        /// <summary>
        /// LocalStack, for Kinesis.
        /// </summary>
        public static async Task<string> GetLocalStackUrlAsync()
        {
            if (_localStack == null)
            {
                _localStack = new LocalStackBuilder("localstack/localstack:3").Build();

                await StartWithRetryAsync(_localStack);
            }

            return _localStack.GetConnectionString();
        }

        /// <summary>
        /// The event hubs of the emulator and their partition counts. The emulator cannot create an event hub
        /// while it runs, so every test that needs one adds it here.
        /// </summary>
        public static readonly (string Name, int Partitions)[] EventHubs =
        [
            ("eh_plain", 1),
            ("eh_route_table", 4),
            ("eh_route_pk", 4),
            ("of_eventhub", 1),
        ];

        /// <summary>
        /// The Event Hubs emulator of Microsoft (with its Azurite container, which the builder starts).
        /// </summary>
        public static async Task<string> GetEventHubsConnectionStringAsync()
        {
            if (_eventHubs == null)
            {
                var configuration = EventHubsServiceConfiguration.Create();

                foreach (var (name, partitions) in EventHubs)
                {
                    configuration = configuration.WithEntity(name, partitions, ["test"]);
                }

                var container = new EventHubsBuilder("mcr.microsoft.com/azure-messaging/eventhubs-emulator:latest")
                    .WithAcceptLicenseAgreement(true)
                    .WithConfigurationBuilder(configuration)
                    .WithPortBinding(FreePort(), 5672)
                    .Build();

                await StartWithRetryAsync(container);

                _eventHubs = container;
            }

            return _eventHubs.GetConnectionString();
        }

        /// <summary>
        /// The containers that the failure tests stop and start again have a fixed host port,
        /// because a random one changes with every start.
        /// </summary>
        public static int FreePort()
        {
            using var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);

            listener.Start();

            return ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        }

        /// <summary>
        /// Stops a container, the way an output goes down, and returns the action that starts it again.
        /// </summary>
        public static async Task<Func<Task>> StopAsync(DotNet.Testcontainers.Containers.IContainer container)
        {
            await container.StopAsync();

            return () => container.StartAsync();
        }

        public static DotNet.Testcontainers.Containers.IContainer DynamoDb => _dynamoDb!;
        public static DotNet.Testcontainers.Containers.IContainer Redis => _redis!;
        public static DotNet.Testcontainers.Containers.IContainer EventHubsEmulator => _eventHubs!;

        /// <summary>
        /// Starting a container pulls its image, and a pull fails now and then, mostly on a CI runner.
        /// </summary>
        private static async Task StartWithRetryAsync(DotNet.Testcontainers.Containers.IContainer container)
        {
            for (var attempt = 1; ; attempt++)
            {
                try
                {
                    await container.StartAsync();
                    return;
                }
                catch (Exception ex) when (attempt < 3)
                {
                    Console.WriteLine($"Starting {container.Image.FullName} failed, trying again: {ex.Message}");

                    await Task.Delay(TimeSpan.FromSeconds(5));
                }
            }
        }

        [AssemblyCleanup]
        public static async Task StopAsync()
        {
            if (_kafka != null) await _kafka.DisposeAsync();
            if (_mongo != null) await _mongo.DisposeAsync();
            if (_dynamoDb != null) await _dynamoDb.DisposeAsync();
            if (_rabbitMq != null) await _rabbitMq.DisposeAsync();
            if (_redis != null) await _redis.DisposeAsync();
            if (_localStack != null) await _localStack.DisposeAsync();
            if (_eventHubs != null) await _eventHubs.DisposeAsync();
            if (_postgres != null) await _postgres.DisposeAsync();
        }
    }
}
