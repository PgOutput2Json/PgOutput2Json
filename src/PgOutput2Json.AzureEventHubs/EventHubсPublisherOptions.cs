using Azure.Messaging.EventHubs.Producer;

namespace PgOutput2Json.AzureEventHubs
{
    public class EventHubsPublisherOptions
    {
        public string ConnectionString { get; set; } = string.Empty;
        public string EventHubName { get; set; } = "pgoutput2json";
        public EventHubProducerClientOptions? ClientOptions { get; set; }
    }
}
