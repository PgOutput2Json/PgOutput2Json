using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;

namespace PgOutput2Json.IntegrationTests.Outputs
{
    [TestClass]
    public class RabbitMqStreamsPublisherTest
    {
        /// <summary>
        /// The broker advertises its own port, not the mapped one, so every connection goes through the resolver.
        /// </summary>
        private static StreamSystemConfig CreateConfig()
        {
            var endpoint = new IPEndPoint(IPAddress.Loopback, Containers.RabbitMqStreamPort);

            return new StreamSystemConfig
            {
                UserName = "rabbitmq",
                Password = "rabbitmq",
                Endpoints = [endpoint],
                AddressResolver = new AddressResolver(endpoint),
            };
        }

        [TestMethod]
        [DataRow("om_stream", false, "[1] [2] [1] [2]")]
        [DataRow("om_stream_t", true, "public.om_stream_t[1] public.om_stream_t[2] public.om_stream_t[1] public.om_stream_t[2]")]
        public async Task RabbitMq_stream_should_get_the_changes_with_the_key_as_the_message_id(string name, bool tableNameInKey, string expectedIds)
        {
            await Containers.GetRabbitMqAsync();

            var system = await StreamSystem.Create(CreateConfig());

            await system.CreateStream(new StreamSpec(name));

            var received = new ConcurrentQueue<(string Body, string? Id)>();

            var consumer = await Consumer.Create(new ConsumerConfig(system, name)
            {
                OffsetSpec = new OffsetTypeFirst(),
                MessageHandler = (_, _, _, message) =>
                {
                    received.Enqueue((Encoding.UTF8.GetString(message.Data.Contents), message.Properties.MessageId?.ToString()));

                    return Task.CompletedTask;
                },
            });

            try
            {
                await MessageScenario.RunAsync(name,
                    builder => builder.UseRabbitMqStreams(options =>
                    {
                        options.StreamName = name;
                        options.WriteTableNameToMessageKey = tableNameInKey;
                        options.StreamSystemConfig = CreateConfig();
                    }),
                    () => Task.FromResult(received.Select(m => m.Body).ToList()));

                Assert.AreEqual(expectedIds, string.Join(" ", received.Select(m => m.Id)));
            }
            finally
            {
                await consumer.Close();
                await system.Close();
            }
        }
    }
}
