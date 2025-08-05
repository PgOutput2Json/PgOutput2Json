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

        public Func<AmazonDynamoDBClient, Task>? PostConnectionSetup { get; set; }
    }
}
