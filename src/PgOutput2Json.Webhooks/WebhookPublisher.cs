using System;
using System.Collections.Generic;
using System.Diagnostics;
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

        private readonly string? _execFilePath;

        private readonly StringBuilder _execFileStdOut = new();
        private readonly StringBuilder _execFileStdErr = new();

        private readonly ILogger<WebhookPublisher>? _logger;

        private readonly StringBuilder _payload = new(1024);

        private readonly Utf8Buffer _utf8 = new();

        private ulong _walSeqFirst;
        private ulong _walSeqLast;

        private HttpClient HttpClient => EnsureHttpClient();

        private readonly byte[] _key = [];
        private readonly List<byte[]> _stdKeys = [];

        public WebhookPublisher(WebhookPublisherOptions options, string? execFilePath, string slotName, int batchSize, ILogger<WebhookPublisher>? logger)
        {
            _userAgent = $"PgHook/{slotName}";
            _options = options;
            _execFilePath = execFilePath;
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

            // the JSON array of the batch, as UTF-8 once: signed and sent as these bytes, also by a retry
            _payload.Append(']');

            var body = _utf8.Encode(_payload);

            // Sign the payload
            string? signature = null;
            if (!string.IsNullOrWhiteSpace(_options.WebhookSecret))
            {
                signature = _options.UseStandardWebhooks
                    ? SignStandard(msgId, timestamp, body.Span)
                    : Sign(body.Span);
            }

            var attempt = 0;

            while (!token.IsCancellationRequested)
            {
                try
                {
                    if (_execFilePath != null)
                    {
                        await PostToLocalFileAsync(_execFilePath, msgId, timestamp, _payload, signature, token)
                            .ConfigureAwait(false);
                    }
                    else
                    {
                        await PostToWebhookAsync(msgId, timestamp, body, signature, token)
                            .ConfigureAwait(false);
                    }

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

        private async Task PostToLocalFileAsync(string filePath, string msgId, string timestamp, StringBuilder body, string? signature, CancellationToken token)
        {
            var psi = new ProcessStartInfo
            {
                FileName = filePath,
                RedirectStandardInput = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                Arguments = _options.ExecFileArgs,
            };

            using var process = new Process { StartInfo = psi };

            _execFileStdOut.Clear();
            _execFileStdErr.Clear();

            process.OutputDataReceived += (s, e) => { if (e.Data != null) _execFileStdOut.AppendLine(e.Data); };
            process.ErrorDataReceived += (s, e) => { if (e.Data != null) _execFileStdErr.AppendLine(e.Data); };

            process.Start();

            // Start async reading
            process.BeginOutputReadLine();
            process.BeginErrorReadLine();

            using var writer = process.StandardInput;

            await writer.WriteLineAsync($"{_headerKeyUserAgent}: {_userAgent}").ConfigureAwait(false);

            if (_options.UseStandardWebhooks)
            {
                await writer.WriteLineAsync($"{_stdHeaderKeyId}: {msgId}").ConfigureAwait(false);
                await writer.WriteLineAsync($"{_stdHeaderKeyTimestamp}: {timestamp}").ConfigureAwait(false);

                if (signature != null)
                {
                    await writer.WriteLineAsync($"{_stdHeaderKeySignature}: {signature}").ConfigureAwait(false);
                }
            }
            else
            {
                await writer.WriteLineAsync($"{_headerKeyTimestamp}: {timestamp}").ConfigureAwait(false);

                if (signature != null)
                {
                    await writer.WriteLineAsync($"{_headerKeySignature}: {signature}").ConfigureAwait(false);
                }
            }

            await writer.WriteLineAsync(body).ConfigureAwait(false);

            writer.Close();  // EOF for child

            await process.WaitForExitAsync(token).ConfigureAwait(false);

            if (_execFileStdOut.Length > 0)
            {
                _logger?.LogInformation(_execFileStdOut.ToString());
            }

            if (_execFileStdErr.Length > 0)
            {
                _logger?.LogError(_execFileStdErr.ToString());
            }

            if (process.ExitCode != 0)
            {
                throw new Exception($"ExecFile returned exit code {process.ExitCode}");
            }
        }

        private async Task PostToWebhookAsync(string msgId, string timestamp, ReadOnlyMemory<byte> body, string? signature, CancellationToken token)
        {
            var content = new ReadOnlyMemoryContent(body);

            content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/json") { CharSet = "utf-8" };

            using var request = new HttpRequestMessage(HttpMethod.Post, _options.WebhookUrl)
            {
                Content = content
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
                    _payload.ToString(),
                    responseBody);
            }

            response.EnsureSuccessStatusCode();
        }

        public Task PublishAsync(JsonMessage jsonMessage, CancellationToken token)
        {
            if (_payload.Length > 0)
            {
                _payload.Append(',');
            }
            else
            {
                _payload.Append('[');
                _walSeqFirst = jsonMessage.TxFinalLsn;
            }

            _walSeqLast = jsonMessage.TxFinalLsn;

            if (_options.UseThinPayload)
            {
                _payload.Append('{');

                _payload.Append("\"w\":");
                _payload.Append(jsonMessage.TxFinalLsn);

                _payload.Append(",\"n\":");
                _payload.Append(jsonMessage.MessageNo);

                _payload.Append(',');

                JsonUtils.WriteText(_payload, jsonMessage.TableName.ToString());
                _payload.Append(':');
                _payload.Append(jsonMessage.KeyKolValue);

                _payload.Append('}');
            }
            else
            {
                // copied from the StringBuilder, without a string of it
                _payload.Append(jsonMessage.Json);
            }

            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }

        public string Sign(ReadOnlySpan<byte> body)
        {
            var hash = HMACSHA256.HashData(_key, body);
            var signature = "sha256=" + BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();

            return signature;
        }

        public string SignStandard(string msgId, string timestamp, ReadOnlySpan<byte> payload)
        {
            // msgId.timestamp.payload, hashed in two parts: no copy of the payload with the prefix in front
            var prefix = _safeUTF8Encoding.GetBytes($"{msgId}.{timestamp}.");

            var result = "";

            foreach (var key in _stdKeys)
            {
                using var hmac = IncrementalHash.CreateHMAC(HashAlgorithmName.SHA256, key);

                hmac.AppendData(prefix);
                hmac.AppendData(payload);

                var hash = hmac.GetHashAndReset();
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
