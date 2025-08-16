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

        private static readonly UTF8Encoding _safeUTF8Encoding = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

        private const string _standardKeyPrefix = "whsec_";

        private const string _headerKeyId = "webhook-id";
        private const string _headerKeySignature = "webhook-signature";
        private const string _headerKeyTimestamp = "webhook-timestamp";

        private readonly WebhookPublisherOptions _options;
        private readonly ILogger<WebhookPublisher>? _logger;

        private readonly StringBuilder _payload = new(1024);

        private ulong _walSeqFirst;
        private ulong _walSeqLast;

        private HttpClient HttpClient => EnsureHttpClient();

        private readonly List<byte[]> _keys = [];

        public WebhookPublisher(WebhookPublisherOptions options, int batchSize, ILogger<WebhookPublisher>? logger)
        {
            _options = options;
            _logger = logger;

            foreach (var secret in options.WebhookSecret.Split(" ", StringSplitOptions.RemoveEmptyEntries))
            {
                _keys.Add(GetKeyFromSecret(secret));
            }
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
            if (_payload.Length == 0) return;

            var msgId = _walSeqFirst + "_" + _walSeqLast;

            var timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString();

            var body = "[" + _payload.ToString() + "]";

            // Sign the payload
            string? signature = null;
            if (!string.IsNullOrWhiteSpace(_options.WebhookSecret))
            {
                signature = Sign(msgId, timestamp, body);
            }

            var attempt = 0;

            while (!token.IsCancellationRequested)
            {
                try
                {
                    using var content = new StringContent(body, Encoding.UTF8, "application/json");

                    content.Headers.Add(_headerKeyId, msgId);
                    content.Headers.Add(_headerKeyTimestamp, timestamp);

                    if (signature != null)
                    {
                        content.Headers.Add(_headerKeySignature, signature);
                    }

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
                    _payload.Clear();
                    _walSeqFirst = _walSeqLast = 0;
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
            if (_payload.Length > 0)
            {
                _payload.Append(',');
            }
            else
            {
                _walSeqFirst = jsonMessage.WalSeqNo;
            }

            _walSeqLast = jsonMessage.WalSeqNo;

            if (_options.UseThinPayload)
            {
                _payload.Append('{');

                _payload.Append("\"w\":");
                _payload.Append(jsonMessage.WalSeqNo);

                _payload.Append(',');

                JsonUtils.WriteText(_payload, jsonMessage.TableName.ToString());
                _payload.Append(':');
                _payload.Append(jsonMessage.KeyKolValue);

                _payload.Append('}');
            }
            else
            {
                _payload.Append(jsonMessage.Json.ToString());
            }

            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }

        public string Sign(string msgId, string timestamp, string payload)
        {
            var toSign = $"{msgId}.{timestamp}.{payload}";
            var toSignBytes = _safeUTF8Encoding.GetBytes(toSign);

            var result = "";

            foreach (var key in _keys)
            {
                using var hmac = new HMACSHA256(key);

                var hash = hmac.ComputeHash(toSignBytes);
                var signature = $"v1,{Convert.ToBase64String(hash)}";

                if (result == "")
                {
                    result = signature;
                }
                else
                {
                    result = string.Join(" ", result, signature);
                }
            }

            return result;
        }

        private static byte[] GetKeyFromSecret(string secret)
        {
            if (secret.StartsWith(_standardKeyPrefix))
            {
                return Convert.FromBase64String(secret[_standardKeyPrefix.Length..]);
            }
            
            return _safeUTF8Encoding.GetBytes(secret);
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
