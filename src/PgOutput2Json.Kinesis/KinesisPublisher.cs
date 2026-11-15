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

        public Task PublishAsync(JsonMessage msg, CancellationToken token)
        {
            var partitionKey = string.Join("", msg.TableName.ToString(), msg.KeyKolValue.ToString());

            var entry = new PutRecordsRequestEntry
            {
                Data = new MemoryStream(Encoding.UTF8.GetBytes(msg.Json.ToString())),
                PartitionKey = partitionKey.Length <= 256 ? partitionKey : partitionKey[..256],
            };

            _buffer.Add(entry);

            return Task.CompletedTask;
        }

        // the limits of one PutRecords request
        internal const int MaxRecordsPerRequest = 500;
        internal const int MaxBytesPerRequest = 5 * 1024 * 1024;

        public async Task ConfirmAsync(CancellationToken token)
        {
            if (_buffer.Count == 0)
                return;

            var client = EnsureClient();

            // a batch can be larger than one request: the requests are sent one after the other, so the records stay in order
            var start = 0;

            while (start < _buffer.Count)
            {
                var count = 0;
                var bytes = 0L;

                while (start + count < _buffer.Count && count < MaxRecordsPerRequest)
                {
                    var entry = _buffer[start + count];
                    var size = entry.Data.Length + Encoding.UTF8.GetByteCount(entry.PartitionKey);

                    // a record larger than the limit goes alone, and Kinesis rejects it
                    if (count > 0 && bytes + size > MaxBytesPerRequest) break;

                    bytes += size;
                    count++;
                }

                var request = new PutRecordsRequest
                {
                    StreamName = _options.StreamName,
                    Records = _buffer.GetRange(start, count),
                };

                var response = await client.PutRecordsAsync(request, token).ConfigureAwait(false);

                if (response.FailedRecordCount > 0)
                {
                    var failed = response.Records.Find(r => r.ErrorCode != null);

                    throw new Exception($"{response.FailedRecordCount} records failed to publish to Kinesis: {failed?.ErrorCode} {failed?.ErrorMessage}");
                }

                start += count;
            }

            _buffer.Clear();
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
