using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace PgOutput2Json.Webhooks
{
    internal class WebhookPublisher : IMessagePublisher
    {
        private static HttpClient? _httpClient;

#if NET9_0_OR_GREATER
        private static readonly Lock _lock = new();
#else
        private static readonly object _lock = new();
#endif

        private readonly WebhookPublisherOptions _options;
        private readonly ILogger<WebhookPublisher>? _logger;
        private readonly List<string> _changes;

        private HttpClient HttpClient => EnsureHttpClient();

        public WebhookPublisher(WebhookPublisherOptions options, int batchSize, ILogger<WebhookPublisher>? logger)
        {
            _options = options;
            _logger = logger;
            _changes = new(batchSize);
        }

        private HttpClient EnsureHttpClient()
        {
            if (_httpClient != null) return _httpClient;

            lock (_lock)
            {
                _httpClient ??= CreateHttpClient(_options);
            }

            return _httpClient;
        }

        public async Task ConfirmAsync(CancellationToken token)
        {
            if (_changes.Count == 0) return;

            // Prepare JSON body
            var body = "[" + string.Join(",", _changes) + "]";


            // Sign the payload
            string? signature = null;
            if (!string.IsNullOrWhiteSpace(_options.WebhookSecret))
            {
                using var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(_options.WebhookSecret ?? ""));

                var hash = hmac.ComputeHash(Encoding.UTF8.GetBytes(body));
                signature = "sha256=" + BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();
            }

            var attempt = 0;

            while (!token.IsCancellationRequested)
            {
                try
                {
                    using var content = new StringContent(body, Encoding.UTF8, "application/json");

                    if (signature != null)
                    {
                        content.Headers.Add("X-Hub-Signature-256", signature);
                    }
                    content.Headers.Add("X-Timestamp", DateTimeOffset.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ"));

                    var response = await HttpClient.PostAsync(_options.WebhookUrl, content, token)
                        .ConfigureAwait(false);

                    if (!response.IsSuccessStatusCode)
                    {
                        var responseBody = await response.Content.ReadAsStringAsync(token)
                            .ConfigureAwait(false);

                        _logger?.LogWarning("[PgHook] Webhook returned {StatusCode}: {Request} {Response}", 
                            response.StatusCode, 
                            body,
                            responseBody);
                    }

                    response.EnsureSuccessStatusCode();

                    // Success — clear and return
                    _changes.Clear();
                    break;
                }
                catch (Exception ex)
                {
                    if (attempt >= _options.RetryDelays.Length) throw;

                    var delay = _options.RetryDelays[attempt];

                    attempt++;

                    _logger?.LogError(ex, "[PgHook] attempt {Attempt} failed. Retrying in {Seconds} seconds", attempt, delay.TotalSeconds);

                    await Task.Delay(delay, token)
                        .ConfigureAwait(false);
                }
            }
        }

        public Task<ulong> GetLastPublishedWalSeqAsync(CancellationToken token)
        {
            return Task.FromResult(0UL); // no de-duplication
        }

        public Task PublishAsync(JsonMessage jsonMessage, CancellationToken token)
        {
            // jsonMessage is reused by the caller so we must copy the values
            _changes.Add(jsonMessage.Json.ToString());

            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }

        private static HttpClient CreateHttpClient(WebhookPublisherOptions options)
        {
            var handler = new SocketsHttpHandler
            {
                PooledConnectionLifetime = options.PooledConnectionLifetime,
                PooledConnectionIdleTimeout = options.PooledConnectionIdleTimeout,

                ConnectTimeout = options.ConnectTimeout,

                KeepAlivePingPolicy = HttpKeepAlivePingPolicy.Always,
                KeepAlivePingDelay = options.KeepAliveDelay,
                KeepAlivePingTimeout = options.KeepAliveTimeout
            };

            return new HttpClient(handler)
            {
                Timeout = options.RequestTimeout
            };
        }

    }
}
