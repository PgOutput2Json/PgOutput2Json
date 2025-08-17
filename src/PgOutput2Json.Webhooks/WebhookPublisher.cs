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

        private const string _stdKeyPrefix = "whsec_";

        private const string _stdHeaderKeyId = "webhook-id";
        private const string _stdHeaderKeySignature = "webhook-signature";
        private const string _stdHeaderKeyTimestamp = "webhook-timestamp";

        private const string _headerKeyUserAgent = "User-Agent";
        private const string _headerKeySignature = "X-Hub-Signature-256";
        private const string _headerKeyTimestamp = "X-Timestamp";

        private readonly string _userAgent;

        private readonly WebhookPublisherOptions _options;
        private readonly ILogger<WebhookPublisher>? _logger;

        private readonly StringBuilder _payload = new(1024);

        private ulong _walSeqFirst;
        private ulong _walSeqLast;

        private HttpClient HttpClient => EnsureHttpClient();

        private readonly byte[] _key = [];
        private readonly List<byte[]> _stdKeys = [];

        public WebhookPublisher(WebhookPublisherOptions options, string slotName, int batchSize, ILogger<WebhookPublisher>? logger)
        {
            _userAgent = $"PgHook/{slotName}";
            _options = options;
            _logger = logger;

            if (_options.UseStandardWebhooks)
            {
                _stdKeys.Add(GetKeyFromSecret(_options.WebhookSecret));

                foreach (var secret in options.OldWebhookSecrets)
                {
                    _stdKeys.Add(GetKeyFromSecret(secret));
                }
            }
            else
            {
                _key = _safeUTF8Encoding.GetBytes(_options.WebhookSecret);
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
                signature = _options.UseStandardWebhooks
                    ? SignStandard(msgId, timestamp, body)
                    : Sign(body);
            }

            var attempt = 0;

            while (!token.IsCancellationRequested)
            {
                try
                {
                    using var request = new HttpRequestMessage(HttpMethod.Post, _options.WebhookUrl)
                    {
                        Content = new StringContent(body, Encoding.UTF8, "application/json")
                    };

                    request.Headers.Add(_headerKeyUserAgent, _userAgent);

                    if (_options.UseStandardWebhooks)
                    {
                        request.Headers.Add(_stdHeaderKeyId, msgId);
                        request.Headers.Add(_stdHeaderKeyTimestamp, timestamp);

                        if (signature != null)
                        {
                            request.Headers.Add(_stdHeaderKeySignature, signature);
                        }
                    }
                    else
                    {
                        request.Headers.Add(_headerKeyTimestamp, timestamp);

                        if (signature != null)
                        {
                            request.Headers.Add(_headerKeySignature, signature);
                        }
                    }

                    var response = await HttpClient.SendAsync(request, token)
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

        public string Sign(string body)
        {
            using var hmac = new HMACSHA256(_key);

            var hash = hmac.ComputeHash(_safeUTF8Encoding.GetBytes(body));
            var signature = "sha256=" + BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();

            return signature;
        }

        public string SignStandard(string msgId, string timestamp, string payload)
        {
            var toSign = $"{msgId}.{timestamp}.{payload}";
            var toSignBytes = _safeUTF8Encoding.GetBytes(toSign);

            var result = "";

            foreach (var key in _stdKeys)
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
            if (secret.StartsWith(_stdKeyPrefix))
            {
                return Convert.FromBase64String(secret[_stdKeyPrefix.Length..]);
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
