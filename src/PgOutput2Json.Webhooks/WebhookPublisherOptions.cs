using System;
using System.Collections.Generic;

namespace PgOutput2Json.Webhooks
{
    public class WebhookPublisherOptions
    {
        public string WebhookUrl { get; set; } = string.Empty;
        public string WebhookSecret { get; set; } = string.Empty;

        /// <summary>
        /// Use standard webhooks. 
        /// See: https://www.standardwebhooks.com/
        /// </summary>
        public bool UseStandardWebhooks { get; set; } = false;

        /// <summary>
        /// This is used only if <see cref="UseStandardWebhooks"/> is true, to support zero downtime secret rotation.
        /// See: https://github.com/standard-webhooks/standard-webhooks/blob/main/spec/standard-webhooks.md#webhook-headers-sending-metadata-to-consumers
        /// </summary>
        public List<string> OldWebhookSecrets { get; set; } = [];

        public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);
        public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(10);

        public TimeSpan KeepAliveDelay { get; set; } = TimeSpan.FromSeconds(60);
        public TimeSpan KeepAliveTimeout { get; set; } = TimeSpan.FromSeconds(10);

        public TimeSpan PooledConnectionLifetime { get; set; } = TimeSpan.FromMinutes(10);
        public TimeSpan PooledConnectionIdleTimeout { get; set; } = TimeSpan.FromMinutes(2);

        /// <summary>
        /// Only send key values
        /// </summary>
        public bool UseThinPayload { get; set; }

        public TimeSpan[] RetryDelays { get; set; } =
        [ 
            TimeSpan.FromSeconds(2),
            TimeSpan.FromSeconds(4),
            TimeSpan.FromSeconds(8),
        ];
    }
}
