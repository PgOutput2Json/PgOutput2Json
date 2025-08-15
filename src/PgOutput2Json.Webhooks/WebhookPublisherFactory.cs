using Microsoft.Extensions.Logging;

namespace PgOutput2Json.Webhooks
{
    internal class WebhookPublisherFactory : IMessagePublisherFactory
    {
        private readonly WebhookPublisherOptions _options;

        public WebhookPublisherFactory(WebhookPublisherOptions options)
        {
            _options = options;
        }

        public IMessagePublisher CreateMessagePublisher(ReplicationListenerOptions listenerOptions, ILoggerFactory? loggerFactory)
        {
            var logger = loggerFactory?.CreateLogger<WebhookPublisher>();

            return new WebhookPublisher(_options, listenerOptions.BatchSize, logger);
        }
    }
}
