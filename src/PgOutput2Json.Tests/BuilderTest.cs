using System;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace PgOutput2Json.Tests
{
    [TestClass]
    public class BuilderTest
    {
        private static PgOutput2JsonBuilder Valid()
        {
            return PgOutput2JsonBuilder.Create()
                .WithPgConnectionString("Host=localhost;Database=test")
                .WithPgPublications("pub")
                .WithMessageHandler((json, table, key, partitionKey) => Task.FromResult(true));
        }

        private static void AssertBuildFails(PgOutput2JsonBuilder builder, string message)
        {
            var exception = Assert.ThrowsExactly<InvalidOperationException>(() => builder.Build());

            Assert.AreEqual(message, exception.Message);
        }

        [TestMethod]
        public void Build_should_succeed_with_a_connection_a_publication_and_an_output()
        {
            using var pgOutput2Json = Valid().Build();

            Assert.IsNotNull(pgOutput2Json);
        }

        [TestMethod]
        public void Build_should_fail_without_a_connection_string()
        {
            var builder = PgOutput2JsonBuilder.Create()
                .WithPgPublications("pub")
                .WithMessageHandler((json, table, key, partitionKey) => Task.FromResult(true));

            AssertBuildFails(builder, "PostgreSQL data source must be configured");
        }

        [TestMethod]
        public void Build_should_fail_without_a_publication()
        {
            AssertBuildFails(Valid().WithPgPublications(), "At least one PostgreSQL publication name must be provided");
        }

        [TestMethod]
        public void Build_should_fail_without_an_output()
        {
            var builder = PgOutput2JsonBuilder.Create()
                .WithPgConnectionString("Host=localhost;Database=test")
                .WithPgPublications("pub");

            AssertBuildFails(builder, "MessagePublisherFactory must be provided");
        }

        [TestMethod]
        public void Build_should_fail_with_a_batch_size_below_one()
        {
            AssertBuildFails(Valid().WithBatchSize(0), "Batch size must be greater than zero");
        }

        [TestMethod]
        public void Build_should_fail_with_a_permanent_slot_without_a_name()
        {
            AssertBuildFails(Valid().WithPgReplicationSlot("", useTemporarySlot: false), "Replication slot name must be provided for permanent slots");
        }

        [TestMethod]
        public void Build_should_fail_with_a_negative_reconnect_delay()
        {
            AssertBuildFails(Valid().WithReconnectDelay(TimeSpan.FromSeconds(-1)), "Reconnect delay must not be negative");
        }
    }
}
