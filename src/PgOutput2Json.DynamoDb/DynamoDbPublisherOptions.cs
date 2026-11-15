using Amazon.DynamoDBv2;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace PgOutput2Json.DynamoDb
{
    public class DynamoDbPublisherOptions
    {
        public required AmazonDynamoDBConfig ClientConfig { get; set; }

        public BillingMode DefaultBillingMode { get; set; } = BillingMode.PAY_PER_REQUEST;
        public Dictionary<string, BillingMode> TableBillingModes { get; set; } = [];

        /// <summary>
        /// The batch writes (25 items each) that are sent at the same time. Two changes of one item are never in flight
        /// together, so their order is kept. More of them use more of the throughput of the tables. Default is 8.
        /// </summary>
        public int MaxParallelBatchWrites { get; set; } = 8;

        public Func<AmazonDynamoDBClient, Task>? PostConnectionSetup { get; set; }
    }
}
