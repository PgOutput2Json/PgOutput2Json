using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace PgOutput2Json.IntegrationTests.Outputs
{
    [TestClass]
    public class KinesisPublisherTest
    {
        [TestMethod]
        public async Task Kinesis_should_put_the_changes_with_the_table_and_key_as_the_partition_key()
        {
            var config = new AmazonKinesisConfig { ServiceURL = await Containers.GetLocalStackUrlAsync(), AuthenticationRegion = "us-east-1" };

            using var client = new AmazonKinesisClient(config);

            await client.CreateStreamAsync(new CreateStreamRequest { StreamName = "om_kinesis", ShardCount = 1 });

            await Wait.UntilAsync(
                async () => (await client.DescribeStreamSummaryAsync(new DescribeStreamSummaryRequest { StreamName = "om_kinesis" })).StreamDescriptionSummary.StreamStatus == StreamStatus.ACTIVE,
                "the stream");

            var shardId = (await client.ListShardsAsync(new ListShardsRequest { StreamName = "om_kinesis" })).Shards[0].ShardId;

            var records = new List<Record>();

            async Task<List<string>> ReadAsync()
            {
                var iterator = (await client.GetShardIteratorAsync(new GetShardIteratorRequest
                {
                    StreamName = "om_kinesis",
                    ShardId = shardId,
                    ShardIteratorType = ShardIteratorType.TRIM_HORIZON,
                })).ShardIterator;

                records = (await client.GetRecordsAsync(new GetRecordsRequest { ShardIterator = iterator })).Records;

                return records.Select(r => Encoding.UTF8.GetString(r.Data.ToArray())).ToList();
            }

            await MessageScenario.RunAsync("om_kinesis",
                builder => builder.UseKinesis(options =>
                {
                    options.StreamName = "om_kinesis";
                    options.KinesisConfig = config;
                }),
                ReadAsync);

            CollectionAssert.AreEqual(
                new[] { "public.om_kinesis[1]", "public.om_kinesis[2]", "public.om_kinesis[1]", "public.om_kinesis[2]" },
                records.Select(r => r.PartitionKey).ToArray());
        }

        [TestMethod]
        public async Task Kinesis_should_put_a_batch_larger_than_one_request_in_order()
        {
            var config = new AmazonKinesisConfig { ServiceURL = await Containers.GetLocalStackUrlAsync(), AuthenticationRegion = "us-east-1" };

            using var client = new AmazonKinesisClient(config);

            await client.CreateStreamAsync(new CreateStreamRequest { StreamName = "om_kinesis_large", ShardCount = 1 });

            await Wait.UntilAsync(
                async () => (await client.DescribeStreamSummaryAsync(new DescribeStreamSummaryRequest { StreamName = "om_kinesis_large" })).StreamDescriptionSummary.StreamStatus == StreamStatus.ACTIVE,
                "the stream");

            // more records than one PutRecords request takes
            const int count = 1_200;

            await using (var publisher = new global::PgOutput2Json.Kinesis.KinesisPublisher(new() { StreamName = "om_kinesis_large", KinesisConfig = config }, null))
            {
                for (var i = 1; i <= count; i++)
                {
                    await publisher.PublishAsync(StoreMessage.Create(100, (ulong)i), default);
                }

                await publisher.ConfirmAsync(default);
            }

            var shardId = (await client.ListShardsAsync(new ListShardsRequest { StreamName = "om_kinesis_large" })).Shards[0].ShardId;

            var iterator = (await client.GetShardIteratorAsync(new GetShardIteratorRequest
            {
                StreamName = "om_kinesis_large",
                ShardId = shardId,
                ShardIteratorType = ShardIteratorType.TRIM_HORIZON,
            })).ShardIterator;

            var messageNumbers = new List<int>();

            for (var attempt = 0; messageNumbers.Count < count && attempt < 50; attempt++)
            {
                var response = await client.GetRecordsAsync(new GetRecordsRequest { ShardIterator = iterator });

                messageNumbers.AddRange(response.Records.Select(r => System.Text.Json.JsonDocument.Parse(r.Data.ToArray()).RootElement.GetProperty("n").GetInt32()));

                iterator = response.NextShardIterator;
            }

            CollectionAssert.AreEqual(Enumerable.Range(1, count).ToArray(), messageNumbers.ToArray());
        }
    }
}
