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

        private readonly bool _useDeduplication;

        private EventHubProducerClient? _producerClient;

        // resolved once and kept in stable numeric order - the routing of a key
        // must resolve to the same partition on every restart
        private List<string>? _partitionIds;

        private readonly List<(EventData EventData, string? PartitionId, string PartitionKey, WalPosition Position)> _buffer = [];

        private readonly Dictionary<string, WalPosition> _lastPublished = new();

        private bool _dedupSkipActive; // per-partition duplicate checks run only while replaying over the last published positions
        private long _dedupSkippedCount;
        private WalPosition _dedupSkipEnd; // highest per-partition watermark from the startup scan - duplicates are impossible past it

        public EventHubsPublisher(EventHubsPublisherOptions options, ILogger<EventHubsPublisher>? logger, bool useDeduplication = true)
        {
            _options = options;
            _logger = logger;
            _useDeduplication = useDeduplication;
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

            // the user-configured partition key columns route the row client-side, so the
            // messages to the same partition can be deduplicated - without them the events
            // keep the legacy table-name partition key and the partition is chosen by the service
            string? partitionId = null;

            if (msg.PartitionKolValue.Length > 0)
            {
                partitionId = await ResolvePartitionIdAsync(msg.PartitionKolValue.ToString(), token).ConfigureAwait(false);
            }

            if (_useDeduplication && _dedupSkipActive && partitionId != null && IsAlreadyPublished(partitionId, msg.TxFinalLsn, msg.MessageNo))
            {
                // already processed
                _dedupSkippedCount++;

                return;
            }

            if (_dedupSkipActive && new WalPosition(msg.TxFinalLsn, msg.MessageNo).IsAfter(_dedupSkipEnd))
            {
                // the replay overlap is over - no further message can be a per-partition
                // duplicate, so summarize the skipped ones and stop the per-message checks
                if (_dedupSkippedCount > 0)
                {
                    _logger?.LogWarning("Deduplication enabled, skipped {SkippedCount} already published messages.", _dedupSkippedCount);
                }

                _dedupSkipActive = false;
            }

            _buffer.Add((eventData, partitionId, tableName, new WalPosition(msg.TxFinalLsn, msg.MessageNo)));
        }

        public async Task ConfirmAsync(CancellationToken token)
        {
            if (_buffer.Count == 0)
                return;

            var client = EnsureClient();

            try
            {
                // group the events by their target - events with a client-resolved partition
                // are sent to it explicitly, the rest keep the table-name partition key
                var eventsByTarget = _buffer.GroupBy(x => (x.PartitionId, x.PartitionKey));

                foreach (var targetGroup in eventsByTarget)
                {
                    var partitionId = targetGroup.Key.PartitionId;

                    var batchOptions = partitionId != null
                        ? new CreateBatchOptions { PartitionId = partitionId }
                        : new CreateBatchOptions { PartitionKey = targetGroup.Key.PartitionKey };

                    var events = targetGroup.Select(x => x.EventData).ToList();

                    await SendEventsInBatchesAsync(client, events, batchOptions, token)
                        .ConfigureAwait(false);

                    // the positions are tracked only after the events are durably sent
                    if (_useDeduplication)
                    {
                        foreach (var entry in targetGroup)
                        {
                            if (entry.PartitionId != null)
                            {
                                TrackWalSeq(entry.PartitionId, entry.Position);
                            }
                        }
                    }
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
            // without deduplication there is no need for the full startup scan
            if (!_useDeduplication) return (0UL, 0UL);

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

        private async Task<string?> ResolvePartitionIdAsync(string routingKey, CancellationToken token)
        {
            var partitionIds = await EnsurePartitionIdsAsync(token).ConfigureAwait(false);

            if (partitionIds.Count == 0) return null;

            // murmur2, mirroring the client-side routing of the Kafka adapter
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

            // stable numeric order - the routing of a key must resolve to the same partition on every restart
            _partitionIds = [.. partitionIds.OrderBy(int.Parse)];

            return _partitionIds;
        }

        private bool IsAlreadyPublished(string partitionId, ulong txFinalLsn, ulong messageNo)
        {
            return _lastPublished.TryGetValue(partitionId, out var last)
                && new WalPosition(txFinalLsn, messageNo).IsDuplicate(last);
        }

        private void TrackWalSeq(string partitionId, WalPosition position)
        {
            // messages are published in order, so the position can only move forward
            if (!_lastPublished.TryGetValue(partitionId, out var last) || position.IsAfter(last))
            {
                _lastPublished[partitionId] = position;
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
        /// <returns>The lowest WAL position found, or (0,0) if no partition holds messages</returns>
        private async Task<(ulong, ulong)> GetMinWalOffsetAsync(string connectionString, string eventHubName, CancellationToken cancellationToken = default)
        {
            await using var consumer = new EventHubConsumerClient(EventHubConsumerClient.DefaultConsumerGroupName, connectionString, eventHubName);

            var partitionIds = await consumer.GetPartitionIdsAsync(cancellationToken)
                    .ConfigureAwait(false);

            // same guard as the routing - a watermark computed from an incomplete
            // partition list would over-report durability and skip live messages
            if (partitionIds.Length == 0)
            {
                throw new Exception("Event Hub returned no partitions - it may not be fully initialized yet.");
            }

            _lastPublished.Clear();

            var min = WalPosition.Zero;
            var hasWatermark = false;

            foreach (var partitionId in partitionIds)
            {
                var partitionProps = await consumer.GetPartitionPropertiesAsync(partitionId, cancellationToken)
                    .ConfigureAwait(false);

                // an empty partition was never written to, or its history was removed by
                // retention - its per-partition watermark is (0,0), so nothing can be
                // skipped for it and every message routed to it must be sent
                if (partitionProps.IsEmpty)
                {
                    _lastPublished[partitionId] = WalPosition.Zero;

                    continue;
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

                _logger?.LogInformation("Last published WAL LSN for partition {Partition}: {LastWalSeq}/{LastMessageNo}", partitionId, position.Value.WalSeq, position.Value.MessageNo);

                _lastPublished[partitionId] = position.Value;

                // the minimum across the partitions is a safe deduplication watermark -
                // everything at or below it is already published to all the partitions
                if (!hasWatermark || position.Value.IsAtOrBelow(min))
                {
                    hasWatermark = true;
                    min = position.Value;
                }
            }

            // once the stream passes the highest per-partition watermark, no further message
            // can be a duplicate for any partition - the per-partition checks only run until then
            _dedupSkipEnd = WalPosition.Zero;

            foreach (var position in _lastPublished.Values)
            {
                if (position.IsAfter(_dedupSkipEnd))
                {
                    _dedupSkipEnd = position;
                }
            }

            _dedupSkippedCount = 0;
            _dedupSkipActive = true;

            _logger?.LogInformation("Last published WAL LSN for {EventHub}: {LastWalSeq}/{LastMessageNo}", eventHubName, min.WalSeq, min.MessageNo);

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