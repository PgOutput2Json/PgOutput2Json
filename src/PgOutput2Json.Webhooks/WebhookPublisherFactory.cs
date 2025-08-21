using Microsoft.Extensions.Logging;
using System;

namespace PgOutput2Json.Webhooks
{
    internal class WebhookPublisherFactory : IMessagePublisherFactory
    {
        private readonly WebhookPublisherOptions _options;
        private readonly string? _execFilePath;

        public WebhookPublisherFactory(WebhookPublisherOptions options)
        {
            _options = options;

            if (Uri.TryCreate(options.WebhookUrl, UriKind.Absolute, out var uri) 
                && uri.Scheme.Equals("file", StringComparison.InvariantCultureIgnoreCase)
                && !string.IsNullOrWhiteSpace(uri.LocalPath))
            {
                _execFilePath = uri.LocalPath;
            }
        }

        public IMessagePublisher CreateMessagePublisher(ReplicationListenerOptions listenerOptions, string slotName, ILoggerFactory? loggerFactory)
        {
            var logger = loggerFactory?.CreateLogger<WebhookPublisher>();

            return new WebhookPublisher(_options, _execFilePath, slotName, listenerOptions.BatchSize, logger);
        }
    }
}
