using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;
using RabbitMQ.Client;

namespace PgOutput2Json.IntegrationTests.Outputs
{
    [TestClass]
    public class RabbitMqPublisherTest
    {
        private sealed record Received(string Body, string RoutingKey, string? Type, bool Persistent);

        /// <summary>
        /// Declares a topic exchange with one queue that gets everything, and returns a reader of that queue.
        /// </summary>
        private static async Task<(ConnectionFactory Factory, Func<Task<List<Received>>> ReadAsync, IAsyncDisposable Connection)> SetUpAsync(string name)
        {
            var container = await Containers.GetRabbitMqAsync();

            var factory = new ConnectionFactory { Uri = new Uri(container.GetConnectionString()) };

            var connection = await factory.CreateConnectionAsync();
            var channel = await connection.CreateChannelAsync();

            await channel.ExchangeDeclareAsync(name, ExchangeType.Topic, durable: true);
            await channel.QueueDeclareAsync($"{name}_q", durable: true, exclusive: false, autoDelete: false);
            await channel.QueueBindAsync($"{name}_q", name, "#");

            var received = new List<Received>();

            async Task<List<Received>> ReadAsync()
            {
                while (await channel.BasicGetAsync($"{name}_q", autoAck: true) is { } result)
                {
                    received.Add(new Received(Encoding.UTF8.GetString(result.Body.Span), result.RoutingKey, result.BasicProperties.Type, result.BasicProperties.Persistent));
                }

                return received.ToList();
            }

            return (factory, ReadAsync, connection);
        }

        [TestMethod]
        public async Task RabbitMq_should_publish_the_changes_with_the_table_as_type_and_routing_key()
        {
            var (factory, readAsync, connection) = await SetUpAsync("om_rabbit");

            await using var _ = connection;

            await MessageScenario.RunAsync("om_rabbit",
                builder => builder.UseRabbitMq(options =>
                {
                    options.ExchangeName = "om_rabbit";
                    options.ConnectionFactory = factory;
                }),
                async () => (await readAsync()).Select(m => m.Body).ToList());

            var received = await readAsync();

            // the routing key is the table and the partition, which is 0 without a partition count
            Assert.IsTrue(received.All(m => m.RoutingKey == "public.om_rabbit.0"), string.Join(" ", received.Select(m => m.RoutingKey)));
            Assert.IsTrue(received.All(m => m.Type == "public.om_rabbit"));
            Assert.IsTrue(received.All(m => m.Persistent));
        }

        [TestMethod]
        public async Task RabbitMq_should_route_by_the_partition_key_with_a_hash_that_does_not_change_between_runs()
        {
            // a partition count that puts the two rows of the scenario into different partitions
            var partitions = Enumerable.Range(2, 20).First(count => Partition("[1]", count) != Partition("[2]", count));

            var (factory, readAsync, connection) = await SetUpAsync("om_rabbit_p");

            await using var _ = connection;

            await MessageScenario.RunAsync("om_rabbit_p",
                builder => builder
                    .WithPartitionKeyFields("public.om_rabbit_p", "id")
                    .UseRabbitMq(options =>
                    {
                        options.ExchangeName = "om_rabbit_p";
                        options.ConnectionFactory = factory;
                        options.TablePartitionCounts["public.om_rabbit_p"] = partitions;
                        options.UsePersistentMessagesByDefault = false;
                    }),
                async () => (await readAsync()).Select(m => m.Body).ToList());

            var received = await readAsync();

            // every change of a row goes to the partition of its partition key

            CollectionAssert.AreEqual(
                new[] { Partition("[1]", partitions), Partition("[2]", partitions), Partition("[1]", partitions), Partition("[2]", partitions) }.Select(p => $"public.om_rabbit_p.{p}").ToArray(),
                received.Select(m => m.RoutingKey).ToArray());

            Assert.IsTrue(received.All(m => !m.Persistent));
        }

        private static int Partition(string key, int count) => (MurmurHash2.Hash(key) & 0x7fffffff) % count;
    }
}
