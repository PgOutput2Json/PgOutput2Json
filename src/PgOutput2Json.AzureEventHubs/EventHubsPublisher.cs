using System;
using System.Collections.Generic;
using System.Linq;
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

        // resolved once and kept in numeric order, so a partition key maps to the same partition on every start
        private List<string>? _partitionIds;

        private readonly List<BufferEntry> _buffer = [];

        private string? _tableName;

        public EventHubsPublisher(EventHubsPublisherOptions options, ILogger<EventHubsPublisher>? logger)
        {
            _options = options;
            _logger = logger;
        }

        private EventHubProducerClient EnsureClient()
        {
            return _producerClient ??= new EventHubProducerClient(_options.ConnectionString, _options.EventHubName, _options.ClientOptions);
        }

        public async Task PublishAsync(JsonMessage msg, CancellationToken token)
        {
            // the table name of the previous message, so that the rows of a table do not each make a string of it
            if (_tableName == null || !msg.TableName.Equals(_tableName.AsSpan()))
            {
                _tableName = msg.TableName.ToString();
            }

            var keyColValue = msg.KeyKolValue.ToString();

            var eventData = new EventData(Utf8Buffer.ToArray(msg.Json))
            {
                MessageId = string.Concat(_tableName, keyColValue)
            };

            eventData.Properties["table"] = _tableName;
            eventData.Properties["keyValue"] = keyColValue;
            eventData.Properties["txFinalLsn"] = msg.TxFinalLsn;
            eventData.Properties["messageNo"] = msg.MessageNo;

            // the partition is chosen here, by the partition key columns or else by the table name: one send per
            // partition then keeps the WAL order of every partition
            var routingKey = msg.PartitionKolValue.Length > 0 ? msg.PartitionKolValue.ToString() : _tableName;

            var partitionId = await ResolvePartitionIdAsync(routingKey, token).ConfigureAwait(false);

            _buffer.Add(new BufferEntry(eventData, partitionId));
        }

        public async Task ConfirmAsync(CancellationToken token)
        {
            if (_buffer.Count == 0)
                return;

            var client = EnsureClient();

            try
            {
                // the partitions are independent, so they are sent at the same time
                await Task.WhenAll(GetSendGroups(_buffer).Select(group => SendEventsInBatchesAsync(
                    client,
                    group.Select(x => x.EventData).ToList(),
                    new CreateBatchOptions { PartitionId = group[0].PartitionId },
                    token))).ConfigureAwait(false);

                _buffer.Clear();
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to send events to Event Hub");
                throw;
            }
        }

        /// <summary>
        /// One group per partition, with the events in buffer (WAL) order.
        /// </summary>
        internal static IEnumerable<List<BufferEntry>> GetSendGroups(List<BufferEntry> buffer)
        {
            return buffer.GroupBy(x => x.PartitionId).Select(x => x.ToList());
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

        private async Task<string> ResolvePartitionIdAsync(string routingKey, CancellationToken token)
        {
            var partitionIds = await EnsurePartitionIdsAsync(token).ConfigureAwait(false);

            // murmur2, the same routing the Kafka adapter uses for partition key columns
            var index = (MurmurHash2.Hash(routingKey) & 0x7fffffff) % partitionIds.Count;

            return partitionIds[index];
        }

        private async Task<List<string>> EnsurePartitionIdsAsync(CancellationToken token)
        {
            if (_partitionIds != null) return _partitionIds;

            var partitionIds = await EnsureClient().GetPartitionIdsAsync(token).ConfigureAwait(false);

            // right after startup (or a transient hiccup) the namespace can report no partitions -
            // routing on an empty list would funnel every event into a single partition, so fail
            // immediately and let the listener reconnect with fresh metadata
            if (partitionIds.Length == 0)
            {
                throw new Exception("Event Hub returned no partitions - it may not be fully initialized yet.");
            }

            _partitionIds = [.. partitionIds.OrderBy(int.Parse)];

            _logger?.LogInformation("Partitions: {PartitionIds}", string.Join(",", _partitionIds));

            return _partitionIds;
        }

        internal readonly record struct BufferEntry(EventData EventData, string PartitionId);
    }
}