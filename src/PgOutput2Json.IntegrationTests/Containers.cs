using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;
using Testcontainers.Kafka;
using Testcontainers.PostgreSql;

namespace PgOutput2Json.IntegrationTests
{
    /// <summary>
    /// One PostgreSQL container for the whole test run. Kafka is started only when a test asks for it.
    /// </summary>
    [TestClass]
    public static class Containers
    {
        private static PostgreSqlContainer? _postgres;
        private static KafkaContainer? _kafka;

        public static string PgConnectionString => _postgres!.GetConnectionString();

        [AssemblyInitialize]
        public static async Task StartAsync(TestContext _)
        {
            _postgres = new PostgreSqlBuilder("postgres:16")
                .WithCommand("-c", "wal_level=logical", "-c", "max_replication_slots=100", "-c", "max_wal_senders=100")
                .Build();

            await _postgres.StartAsync();
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

        [AssemblyCleanup]
        public static async Task StopAsync()
        {
            if (_kafka != null) await _kafka.DisposeAsync();
            if (_postgres != null) await _postgres.DisposeAsync();
        }
    }
}
