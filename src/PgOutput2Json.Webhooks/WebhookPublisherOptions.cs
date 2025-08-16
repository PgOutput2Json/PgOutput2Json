using System;

namespace PgOutput2Json.Webhooks
{
    public class WebhookPublisherOptions
    {
        public string WebhookUrl { get; set; } = string.Empty;
        public string WebhookSecret { get; set; } = string.Empty;

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
