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

            var eventData = new EventData(msg.Json.ToString())
            {
                MessageId = string.Join("", tableName, keyColValue)
            };

            eventData.Properties["table"] = tableName;
            eventData.Properties["keyValue"] = keyColValue;
            eventData.Properties["txFinalLsn"] = msg.TxFinalLsn;
            eventData.Properties["messageNo"] = msg.MessageNo;

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

        public async Task<(ulong, ulong)> GetLastPublishedWalSeqAsync(CancellationToken token)
        {
            return await GetMinWalOffsetAsync(_options.ConnectionString, _options.EventHubName, token)
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
        /// Reads the last message from each partition and returns the lowest WAL position -
        /// everything at or below it is already published to all partitions, so it is the
        /// safe resume point. Optimized for single publisher scenario - only reads one
        /// message per partition.
        /// </summary>
        /// <param name="connectionString">Event Hubs connection string</param>
        /// <param name="eventHubName">Event Hub name</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>The lowest WAL position found, or (0,0) if any partition is empty</returns>
        private async Task<(ulong, ulong)> GetMinWalOffsetAsync(string connectionString, string eventHubName, CancellationToken cancellationToken = default)
        {
            await using var consumer = new EventHubConsumerClient(EventHubConsumerClient.DefaultConsumerGroupName, connectionString, eventHubName);

            var partitionIds = await consumer.GetPartitionIdsAsync(cancellationToken)
                    .ConfigureAwait(false);

            var min = WalPosition.Zero;
            var hasWatermark = false;

            foreach (var partitionId in partitionIds)
            {
                var partitionProps = await consumer.GetPartitionPropertiesAsync(partitionId, cancellationToken)
                    .ConfigureAwait(false);

                // an empty partition contributes (0,0) to the minimum, which forces a full
                // replay - duplicates are safe, a wrong watermark is data loss
                if (partitionProps.IsEmpty)
                {
                    return (0UL, 0UL);
                }

                // Read from the last sequence number (the very last message)
                var lastEventPosition = EventPosition.FromSequenceNumber(partitionProps.LastEnqueuedSequenceNumber);

                var readOptions = new ReadEventOptions
                {
                    MaximumWaitTime = TimeSpan.FromSeconds(2) // Short timeout since we only need one message
                };

                WalPosition? position = null;

                await foreach (var partitionEvent in consumer.ReadEventsFromPartitionAsync(partitionId, lastEventPosition, readOptions, cancellationToken))
                {
                    position = new WalPosition(GetULongPropValue(partitionEvent, "txFinalLsn"), GetULongPropValue(partitionEvent, "messageNo"));

                    break;
                }

                if (position == null)
                {
                    throw new Exception($"Could not read the last message from Event Hub partition {partitionId} - the partition reports sequence number {partitionProps.LastEnqueuedSequenceNumber}, but no event was received in time.");
                }

                // the minimum across the partitions is a safe deduplication watermark -
                // everything at or below it is already published to all the partitions
                if (!hasWatermark || position.Value.IsAtOrBelow(min))
                {
                    hasWatermark = true;
                    min = position.Value;
                }
            }

            return (min.WalSeq, min.MessageNo);
        }

        private static ulong GetULongPropValue(PartitionEvent partitionEvent, string propName)
        {
            partitionEvent.Data.Properties.TryGetValue(propName, out var walOffsetProp);

            ulong propValue;

            if (walOffsetProp == null)
            {
                propValue = 0UL;
            }
            else if (walOffsetProp is ulong value)
            {
                propValue = value;
            }
            else
            {
                ulong.TryParse(walOffsetProp.ToString(), out propValue);
            }

            return propValue;
        }
    }
}
