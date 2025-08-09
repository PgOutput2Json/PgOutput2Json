using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Azure.Messaging.EventHubs.Consumer;

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
            var tableName = msg.TableName.ToString();
            var keyColValue = msg.KeyKolValue.ToString();

            var eventData = new EventData(msg.Json.ToString());

            eventData.MessageId = string.Join("", tableName, keyColValue);

            eventData.Properties["table"] = tableName;
            eventData.Properties["keyValue"] = keyColValue;
            eventData.Properties["walOffset"] = msg.WalSeqNo;

            _buffer.Add((eventData, tableName));

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

        public async Task<ulong> GetLastPublishedWalSeqAsync(CancellationToken token)
        {
            return await GetMaxWalOffsetAsync(_options.ConnectionString, _options.EventHubName, token)
                .ConfigureAwait(false);
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

        /// <summary>
        /// Reads the last message from each partition and returns the largest WAL offset.
        /// Optimized for single publisher scenario - only reads one message per partition.
        /// </summary>
        /// <param name="connectionString">Event Hubs connection string</param>
        /// <param name="eventHubName">Event Hub name</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>The largest WAL offset found, or 0 if no messages found</returns>
        private static async Task<ulong> GetMaxWalOffsetAsync(string connectionString, string eventHubName, CancellationToken cancellationToken = default)
        {
            await using var consumer = new EventHubConsumerClient(EventHubConsumerClient.DefaultConsumerGroupName, connectionString, eventHubName);

            var partitionIds = await consumer.GetPartitionIdsAsync(cancellationToken)
                    .ConfigureAwait(false);

            var maxWalOffset = 0UL;

            foreach (var partitionId in partitionIds)
            {
                try
                {
                    // Check if partition has any messages
                    var partitionProps = await consumer.GetPartitionPropertiesAsync(partitionId, cancellationToken)
                        .ConfigureAwait(false);

                    if (partitionProps.IsEmpty)
                    {
                        return 0L; // No messages in this partition
                    }

                    // Read from the last sequence number (the very last message)
                    var lastEventPosition = EventPosition.FromSequenceNumber(partitionProps.LastEnqueuedSequenceNumber);

                    var readOptions = new ReadEventOptions
                    {
                        MaximumWaitTime = TimeSpan.FromSeconds(2) // Short timeout since we only need one message
                    };

                    await foreach (var partitionEvent in consumer.ReadEventsFromPartitionAsync(partitionId, lastEventPosition, readOptions, cancellationToken))
                    {
                        partitionEvent.Data.Properties.TryGetValue("walOffset", out var walOffsetProp);

                        ulong walOffset;

                        if (walOffsetProp == null)
                        {
                            walOffset = 0UL;
                        }
                        else if (walOffsetProp is ulong value)
                        {
                            walOffset = value;
                        }
                        else
                        {
                            ulong.TryParse(walOffsetProp.ToString(), out walOffset);
                        }

                        if (walOffset > maxWalOffset)
                        {
                            maxWalOffset = walOffset;
                        }

                        break;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Warning: Could not read from partition {partitionId}: {ex.Message}");
                    return 0UL; // Return 0 for failed partitions, don't crash the whole operation
                }
            };

            return maxWalOffset;
        }
    }
}
