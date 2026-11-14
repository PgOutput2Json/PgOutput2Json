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

            // the user-configured partition key columns route the row client-side - without them the
            // events keep the table-name partition key and the partition is chosen by the service
            string? partitionId = null;

            if (msg.PartitionKolValue.Length > 0)
            {
                partitionId = await ResolvePartitionIdAsync(msg.PartitionKolValue.ToString(), token).ConfigureAwait(false);
            }

            _buffer.Add(new BufferEntry(eventData, partitionId, tableName));
        }

        public async Task ConfirmAsync(CancellationToken token)
        {
            if (_buffer.Count == 0)
                return;

            var client = EnsureClient();

            try
            {
                foreach (var group in GetSendGroups(_buffer))
                {
                    var partitionId = group[0].PartitionId;

                    var batchOptions = partitionId != null
                        ? new CreateBatchOptions { PartitionId = partitionId }
                        : new CreateBatchOptions { PartitionKey = group[0].PartitionKey };

                    var events = group.Select(x => x.EventData).ToList();

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

        /// <summary>
        /// Splits the buffer into groups that are sent one after another, so that every partition
        /// receives its events in WAL order.
        /// </summary>
        internal static IEnumerable<List<BufferEntry>> GetSendGroups(List<BufferEntry> buffer)
        {
            // all the events have a client-resolved partition - one group per partition keeps the order
            if (buffer.TrueForAll(x => x.PartitionId != null))
            {
                return buffer.GroupBy(x => x.PartitionId).Select(x => x.ToList());
            }

            // events routed by the table-name partition key land in a partition chosen by the service, and several
            // tables can share one - the order is kept only by sending in buffer order, with a new group
            // whenever the target changes
            var groups = new List<List<BufferEntry>>();

            foreach (var entry in buffer)
            {
                var last = groups.Count > 0 ? groups[^1] : null;

                if (last == null || !HasSameTarget(last[0], entry))
                {
                    groups.Add(last = []);
                }

                last.Add(entry);
            }

            return groups;
        }

        private static bool HasSameTarget(BufferEntry a, BufferEntry b)
        {
            // the table-name partition key is used only without a client-resolved partition
            return a.PartitionId != null || b.PartitionId != null
                ? a.PartitionId == b.PartitionId
                : a.PartitionKey == b.PartitionKey;
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

        private async Task<string?> ResolvePartitionIdAsync(string routingKey, CancellationToken token)
        {
            var partitionIds = await EnsurePartitionIdsAsync(token).ConfigureAwait(false);

            if (partitionIds.Count == 0) return null;

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

            _logger?.LogInformation("Partitions: {PartitionIds}", _partitionIds.Aggregate("", (acc, x) => acc + x + ","));

            return _partitionIds;
        }

        internal readonly record struct BufferEntry(EventData EventData, string? PartitionId, string PartitionKey);
    }
}