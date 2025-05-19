using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

using Amazon.Kinesis;
using Amazon.Kinesis.Model;

namespace PgOutput2Json.Kinesis
{
    public class KinesisPublisher : IMessagePublisher
    {
        private readonly KinesisPublisherOptions _options;
        private readonly ILogger<KinesisPublisher>? _logger;

        private AmazonKinesisClient? _kinesisClient;

        private readonly List<PutRecordsRequestEntry> _buffer = [];

        public KinesisPublisher(KinesisPublisherOptions options, ILogger<KinesisPublisher>? logger)
        {
            _options = options;
            _logger = logger;
        }

        private IAmazonKinesis EnsureClient()
        {
            return _kinesisClient ??= new AmazonKinesisClient(_options.KinesisConfig);
        }

        public Task PublishAsync(ulong walSeqNo, string json, string tableName, string keyColumnValue, int partition, CancellationToken token)
        {
            var payload = new
            {
                WalSeqNo = walSeqNo,
                Table = tableName,
                Key = keyColumnValue,
                Data = json
            };

            var jsonString = System.Text.Json.JsonSerializer.Serialize(payload);
            var bytes = Encoding.UTF8.GetBytes(jsonString);
            var partitionKey = string.Join("", tableName, keyColumnValue);

            var entry = new PutRecordsRequestEntry
            {
                Data = new MemoryStream(bytes),
                PartitionKey = partitionKey.Length <= 256 ? partitionKey : partitionKey[..256],
            };

            _buffer.Add(entry);

            return Task.CompletedTask;
        }

        public async Task ConfirmAsync(CancellationToken token)
        {
            if (_buffer.Count == 0)
                return;

            var client = EnsureClient();

            var request = new PutRecordsRequest
            {
                StreamName = _options.StreamName,
                Records = _buffer
            };

            var response = await client.PutRecordsAsync(request, token).ConfigureAwait(false);

            if (response.FailedRecordCount > 0)
            {
                throw new Exception($"{response.FailedRecordCount} records failed to publish to Kinesis.");
            }

            _buffer.Clear();
        }

        public Task<ulong> GetLastPublishedWalSeqAsync(CancellationToken token)
        {
            return Task.FromResult(0UL);
        }

        public ValueTask DisposeAsync()
        {
            try
            {
                _kinesisClient?.Dispose();
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "KinesisClient disposal failed");
            }

            _kinesisClient = null;

            return ValueTask.CompletedTask;
        }
    }
}
