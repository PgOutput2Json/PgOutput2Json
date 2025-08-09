using System;
using PgOutput2Json.AzureEventHubs;

namespace PgOutput2Json
{
    public static class PgOutput2JsonBuilderExtensions
    {
        public static PgOutput2JsonBuilder UseEventHubs(this PgOutput2JsonBuilder builder, 
            Action<EventHubsPublisherOptions>? configureAction = null)
        {
            var options = new EventHubsPublisherOptions();

            configureAction?.Invoke(options);

            if (string.IsNullOrEmpty(options.ConnectionString))
            {
                throw new ArgumentException("EventHubs ConnectionString must be provided", nameof(options.ConnectionString));
            }

            if (string.IsNullOrEmpty(options.EventHubName))
            {
                throw new ArgumentException("EventHub Name must be provided", nameof(options.EventHubName));
            }

            builder.WithMessagePublisherFactory(new EventHubsPublisherFactory(options));

            return builder;
        }
    }
}
