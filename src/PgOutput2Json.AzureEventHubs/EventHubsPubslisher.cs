using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;

namespace PgOutput2Json.AzureEventHubs
{
    public class EventHubsPublisher : IMessagePublisher
    {
        private readonly EventHubsPublisherOptions _options;
        private readonly ILogger<EventHubsPublisher>? _logger;

        private EventHubProducerClient? _producerClient;

        private readonly List<(EventData EventData, string PartitionKey)> _buffer = [];

        public EventHubsPublisher(EventHubsPublisherOptions options, ILogger<EventHubsPublisher>? logger)
        {
            _options = options;
            _logger = logger;
        }

        private EventHubProducerClient EnsureClient()
        {
            return _producerClient ??= new EventHubProducerClient(_options.ConnectionString, _options.EventHubName, _options.ClientOptions);
        }

        public Task PublishAsync(JsonMessage msg, CancellationToken token)
        {
            var partitionKey = string.Join("", msg.TableName.ToString(), msg.KeyKolValue.ToString());

            var eventData = new EventData(Encoding.UTF8.GetBytes(msg.Json.ToString()));

            // Add custom properties for better tracking
            eventData.Properties["table"] = msg.TableName.ToString();
            eventData.Properties["walOffset"] = msg.WalSeqNo.ToString();
            eventData.Properties["partitionKey"] = partitionKey;

            _buffer.Add((eventData, partitionKey));

            return Task.CompletedTask;
        }

        public async Task ConfirmAsync(CancellationToken token)
        {
            if (_buffer.Count == 0)
                return;

            var client = EnsureClient();

            try
            {
                // Group events by partition key to send them in optimal batches
                var eventsByPartition = _buffer.GroupBy(x => x.PartitionKey);

                foreach (var partitionGroup in eventsByPartition)
                {
                    var partitionKey = partitionGroup.Key;
                    var events = partitionGroup.Select(x => x.EventData).ToList();

                    var batchOptions = new CreateBatchOptions
                    {
                        PartitionKey = partitionKey
                    };

                    await SendEventsInBatchesAsync(client, events, batchOptions, token)
                        .ConfigureAwait(false);
                }

                _buffer.Clear();
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to send events to Event Hub");
                throw;
            }
        }

        public Task<ulong> GetLastPublishedWalSeqAsync(CancellationToken token)
        {
            return Task.FromResult(0UL);
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                if (_producerClient != null)
                {
                    await _producerClient.DisposeAsync().ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "EventHubProducerClient disposal failed");
            }

            _producerClient = null;
        }

        private static async Task SendEventsInBatchesAsync(EventHubProducerClient client, List<EventData> events, CreateBatchOptions batchOptions, CancellationToken token)
        {
            var eventIndex = 0;

            while (eventIndex < events.Count)
            {
                using var eventBatch = await client.CreateBatchAsync(batchOptions, token)
                    .ConfigureAwait(false);

                // Add as many events as possible to the current batch
                while (eventIndex < events.Count)
                {
                    if (!eventBatch.TryAdd(events[eventIndex]))
                    {
                        // If the batch is empty and we can't add the event, it means the event is too large
                        if (eventBatch.Count == 0)
                        {
                            throw new Exception($"Event at index {eventIndex} is too large to fit in a batch. Event size exceeds the maximum allowed size.");
                        }
                        
                        // Otherwise, the batch is full, so we'll send it and create a new batch for remaining events
                        break;
                    }
                    
                    eventIndex++;
                }

                // Send the batch if it contains any events
                if (eventBatch.Count > 0)
                {
                    await client.SendAsync(eventBatch, token).ConfigureAwait(false);
                }
            }
        }
    }
}
