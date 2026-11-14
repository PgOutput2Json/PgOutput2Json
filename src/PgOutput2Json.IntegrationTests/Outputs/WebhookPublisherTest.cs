using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace PgOutput2Json.IntegrationTests.Outputs
{
    [TestClass]
    public class WebhookPublisherTest
    {
        private sealed record Request(string Body, Dictionary<string, string> Headers);

        /// <summary>
        /// A local HTTP endpoint that keeps the requests it accepted. The status of each request comes from
        /// <see cref="NextStatus"/>, which gets the number of requests received before it.
        /// </summary>
        private sealed class Endpoint : IDisposable
        {
            private readonly HttpListener _listener = new();
            private readonly ConcurrentQueue<Request> _accepted = new();
            private int _received;

            public string Url { get; }
            public Func<int, int> NextStatus { get; set; } = _ => 200;
            public int Received => _received;
            public List<Request> Accepted => _accepted.ToList();

            public Endpoint()
            {
                using (var socket = new TcpListener(IPAddress.Loopback, 0))
                {
                    socket.Start();
                    Url = $"http://127.0.0.1:{((IPEndPoint)socket.LocalEndpoint).Port}/hook/";
                    socket.Stop();
                }

                _listener.Prefixes.Add(Url);
                _listener.Start();

                _ = Task.Run(ServeAsync);
            }

            private async Task ServeAsync()
            {
                while (_listener.IsListening)
                {
                    HttpListenerContext context;

                    try
                    {
                        context = await _listener.GetContextAsync();
                    }
                    catch (Exception) when (!_listener.IsListening)
                    {
                        return;
                    }

                    using var reader = new StreamReader(context.Request.InputStream, Encoding.UTF8);

                    var body = await reader.ReadToEndAsync();
                    var status = NextStatus(Interlocked.Increment(ref _received) - 1);

                    if (status == 200)
                    {
                        _accepted.Enqueue(new Request(body, context.Request.Headers.AllKeys.ToDictionary(k => k!, k => context.Request.Headers[k]!, StringComparer.OrdinalIgnoreCase)));
                    }

                    context.Response.StatusCode = status;
                    context.Response.Close();
                }
            }

            /// <summary>
            /// The messages of all the accepted requests: each request body is a JSON array of messages.
            /// </summary>
            public Task<List<string>> ReadAsync()
            {
                var messages = new List<string>();

                foreach (var request in Accepted)
                {
                    using var json = JsonDocument.Parse(request.Body);

                    messages.AddRange(json.RootElement.EnumerateArray().Select(m => m.GetRawText()));
                }

                return Task.FromResult(messages);
            }

            public void Dispose()
            {
                _listener.Stop();
                _listener.Close();
            }
        }

        [TestMethod]
        public async Task Webhook_should_post_the_batch_as_an_array_with_a_signature()
        {
            using var endpoint = new Endpoint();

            await MessageScenario.RunAsync("om_hook",
                builder => builder.UseWebhook(endpoint.Url, options => options.WebhookSecret = "secret"),
                endpoint.ReadAsync);

            foreach (var request in endpoint.Accepted)
            {
                using var hmac = new HMACSHA256(Encoding.UTF8.GetBytes("secret"));

                var expected = "sha256=" + Convert.ToHexString(hmac.ComputeHash(Encoding.UTF8.GetBytes(request.Body))).ToLowerInvariant();

                Assert.AreEqual(expected, request.Headers["X-Hub-Signature-256"]);
                Assert.AreEqual("PgHook/om_hook_slot", request.Headers["User-Agent"]);

                var timestamp = DateTimeOffset.FromUnixTimeSeconds(long.Parse(request.Headers["X-Timestamp"]));

                Assert.IsTrue(Math.Abs((DateTimeOffset.UtcNow - timestamp).TotalMinutes) < 5, $"timestamp {timestamp}");
            }
        }

        [TestMethod]
        public async Task Webhook_should_sign_standard_webhooks_with_the_id_and_the_timestamp()
        {
            using var endpoint = new Endpoint();

            var key = RandomNumberGenerator.GetBytes(24);

            await MessageScenario.RunAsync("om_hook_std",
                builder => builder.UseWebhook(endpoint.Url, options =>
                {
                    options.UseStandardWebhooks = true;
                    options.WebhookSecret = "whsec_" + Convert.ToBase64String(key);
                }),
                endpoint.ReadAsync);

            foreach (var request in endpoint.Accepted)
            {
                var id = request.Headers["webhook-id"];
                var timestamp = request.Headers["webhook-timestamp"];

                using var hmac = new HMACSHA256(key);

                var expected = "v1," + Convert.ToBase64String(hmac.ComputeHash(Encoding.UTF8.GetBytes($"{id}.{timestamp}.{request.Body}")));

                Assert.AreEqual(expected, request.Headers["webhook-signature"]);
            }
        }

        [TestMethod]
        public async Task Webhook_should_retry_a_failed_request_with_the_same_batch()
        {
            using var endpoint = new Endpoint { NextStatus = received => received < 2 ? 500 : 200 };

            await MessageScenario.RunAsync("om_hook_retry",
                builder => builder.UseWebhook(endpoint.Url, options => options.RetryDelays = [TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(100)]),
                endpoint.ReadAsync,
                expectErrors: true);

            Assert.IsTrue(endpoint.Received >= 3, $"{endpoint.Received} requests");
        }

        [TestMethod]
        public async Task Webhook_that_keeps_failing_should_not_confirm_and_deliver_once_it_works()
        {
            var down = true;

            using var endpoint = new Endpoint { NextStatus = _ => down ? 503 : 200 };

            await FailureScenario.RunAsync("of_hook",
                // one retry, then the batch fails and the listener starts over
                builder => builder.UseWebhook(endpoint.Url, options => options.RetryDelays = [TimeSpan.FromMilliseconds(100)]),
                breakAsync: () => Task.FromResult<Func<Task>>(() =>
                {
                    down = false;

                    return Task.CompletedTask;
                }),
                readIdsAsync: async () => FailureScenario.Ids(await endpoint.ReadAsync()),
                breakBeforeStart: true);

            Assert.IsTrue(endpoint.Received >= 3, $"{endpoint.Received} requests");
        }

        [TestMethod]
        public async Task Webhook_with_a_thin_payload_should_send_only_the_position_the_table_and_the_key()
        {
            using var endpoint = new Endpoint();

            await Db.ExecuteAsync(@"
                CREATE TABLE om_hook_thin (id int PRIMARY KEY, v text);
                CREATE PUBLICATION om_hook_thin_pub FOR TABLE om_hook_thin;");

            await Db.CreateSlotAsync("om_hook_thin_slot");

            var builder = PgOutput2JsonBuilder.Create()
                .WithPgConnectionString(Containers.PgConnectionString)
                .WithPgPublications("om_hook_thin_pub")
                .WithPgReplicationSlot("om_hook_thin_slot")
                .UseWebhook(endpoint.Url, options => options.UseThinPayload = true);

            await using (new RunningListener(builder))
            {
                await Db.ExecuteAsync("INSERT INTO om_hook_thin VALUES (1, 'a'), (2, 'b')");

                await Wait.UntilAsync(async () => (await endpoint.ReadAsync()).Count >= 2, "2 messages");
            }

            var messages = (await endpoint.ReadAsync()).Select(m => System.Text.RegularExpressions.Regex.Replace(m, "\"w\":\\d+", "\"w\":0")).ToArray();

            CollectionAssert.AreEqual(
                new[] { """{"w":0,"n":1,"public.om_hook_thin":[1]}""", """{"w":0,"n":2,"public.om_hook_thin":[2]}""" },
                messages);
        }
    }
}
