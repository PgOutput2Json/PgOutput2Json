using System;
using PgOutput2Json.Webhooks;

namespace PgOutput2Json
{
    public static class PgOutput2JsonBuilderExtensions
    {
        public static PgOutput2JsonBuilder UseWebhook(this PgOutput2JsonBuilder builder, string webhookUrl,
            Action<WebhookPublisherOptions>? configureAction = null)
        {
            if (string.IsNullOrWhiteSpace(webhookUrl))
            {
                throw new ArgumentException("WebhookUrl must be provided", nameof(webhookUrl));
            }

            // it does not know its last position: nothing to skip. WithDeduplication after UseWebhook changes it
            builder.WithDeduplication(false);

            var options = new WebhookPublisherOptions() { WebhookUrl = webhookUrl };

            configureAction?.Invoke(options);

            builder.WithMessagePublisherFactory(new WebhookPublisherFactory(options));

            return builder;
        }
    }
}
