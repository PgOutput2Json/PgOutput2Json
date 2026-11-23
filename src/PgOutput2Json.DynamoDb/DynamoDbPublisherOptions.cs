using Amazon.DynamoDBv2;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace PgOutput2Json.DynamoDb
{
    public class DynamoDbPublisherOptions
    {
        public required AmazonDynamoDBConfig ClientConfig { get; set; }

        /// <summary>
        /// Required. The name of this subscription (one source database and replication slot) in the store: the position
        /// of the last saved change is kept under it. Every source that writes into the same store needs its own.
        /// </summary>
        public string SubscriptionName { get; set; } = "";

        public BillingMode DefaultBillingMode { get; set; } = BillingMode.PAY_PER_REQUEST;
        /// <summary>The billing mode of a table the output creates, by the PostgreSQL table (schema.table).</summary>
        public Dictionary<string, BillingMode> TableBillingModes { get; set; } = [];

        /// <summary>
        /// Returns the name of the DynamoDB table for a PostgreSQL table, from its schema and its name.
        /// By default a table of the public schema keeps its name, and any other becomes "schema.table".
        /// Two PostgreSQL tables with the same DynamoDB name are an error.
        /// </summary>
        public Func<string, string, string> GetTableName { get; set; } = (schemaName, tableName) => TargetNames.Default(schemaName, tableName, ".");

        /// <summary>
        /// The batch writes (25 items each) that are sent at the same time. Two changes of one item are never in flight
        /// together, so their order is kept. More of them use more of the throughput of the tables. Default is 8.
        /// </summary>
        public int MaxParallelBatchWrites { get; set; } = 8;

        public Func<AmazonDynamoDBClient, Task>? PostConnectionSetup { get; set; }
    }
}
