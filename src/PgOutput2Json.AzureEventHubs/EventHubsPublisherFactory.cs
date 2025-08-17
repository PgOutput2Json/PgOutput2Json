using Microsoft.Extensions.Logging;

namespace PgOutput2Json.AzureEventHubs
{
    internal class EventHubsPublisherFactory : IMessagePublisherFactory
    {
        private readonly EventHubsPublisherOptions _options;

        public EventHubsPublisherFactory(EventHubsPublisherOptions options)
        {
            _options = options;
        }

        public IMessagePublisher CreateMessagePublisher(ReplicationListenerOptions listenerOptions, string slotName, ILoggerFactory? loggerFactory)
        {
            return new EventHubsPublisher(_options, loggerFactory?.CreateLogger<EventHubsPublisher>());
        }
    }
}
