using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace PgOutput2Json.IntegrationTests.Outputs
{
    [TestClass]
    public class DynamoDbStoreTest
    {
        private static string Describe(AttributeValue value)
        {
            if (value.NULL == true) return "NULL";
            if (value.BOOL != null) return value.BOOL.Value ? "true" : "false";
            // DynamoDB keeps a number without its trailing zeros; DynamoDB Local does so in a batch write only
            if (value.N != null) return value.N.Contains('.') ? value.N.TrimEnd('0').TrimEnd('.') : value.N;
            if (value.B != null) return "x" + Convert.ToHexString(value.B.ToArray());
            if (value.S != null) return value.S.Length > 100 ? $"{value.S[0]}*{value.S.Length}" : $"'{value.S}'";

            return "?";
        }

        [TestMethod]
        public async Task Position_saved_in_two_values_should_be_read_and_saved_again_as_one()
        {
            var config = new AmazonDynamoDBConfig { ServiceURL = await Containers.GetDynamoDbUrlAsync() };

            using var client = new AmazonDynamoDBClient(config);

            var options = new global::PgOutput2Json.DynamoDb.DynamoDbPublisherOptions { ClientConfig = config };

            Task PutAsync(string key, string value) => client.PutItemAsync(new PutItemRequest
            {
                TableName = "__pg2j_config",
                Item = new() { ["ConfigKey"] = new AttributeValue { S = key }, ["Value"] = new AttributeValue { S = value } },
            });

            await using (var publisher = new global::PgOutput2Json.DynamoDb.DynamoDbPublisher(options, null))
            {
                // creates the config table; the other store tests leave their own position in it
                await publisher.GetLastPublishedWalSeqAsync(default);

                // a store of an earlier version: the two parts of the position under keys of their own
                await client.DeleteItemAsync("__pg2j_config", new() { ["ConfigKey"] = new AttributeValue { S = "Position" } });
                await PutAsync("WalEnd", "200");
                await PutAsync("MessageNo", "5");

                Assert.AreEqual((200UL, 5UL), await publisher.GetLastPublishedWalSeqAsync(default));

                await publisher.PublishAsync(StoreMessage.Create(300, 1), default);
                await publisher.ConfirmAsync(default);
            }

            var position = await client.GetItemAsync("__pg2j_config", new() { ["ConfigKey"] = new AttributeValue { S = "Position" } }, true);

            Assert.AreEqual("300/1", position.Item["Value"].S);

            await using (var publisher = new global::PgOutput2Json.DynamoDb.DynamoDbPublisher(options, null))
            {
                Assert.AreEqual((300UL, 1UL), await publisher.GetLastPublishedWalSeqAsync(default));
            }
        }

        [TestMethod]
        public async Task DynamoDb_should_mirror_the_tables()
        {
            var config = new AmazonDynamoDBConfig { ServiceURL = await Containers.GetDynamoDbUrlAsync() };

            using var client = new AmazonDynamoDBClient(config);

            // the items of a table, ordered by id, with the attributes in name order
            async Task<string> ReadAsync(string table)
            {
                List<Dictionary<string, AttributeValue>> items;

                try
                {
                    items = (await client.ScanAsync(new ScanRequest { TableName = table, ConsistentRead = true })).Items;
                }
                catch (ResourceNotFoundException)
                {
                    return "(no table)";
                }

                return string.Join("\n", items
                    .OrderBy(item => int.Parse(item["id"].N))
                    .Select(item => string.Join(" ", item.OrderBy(a => a.Key, StringComparer.Ordinal).Select(a => $"{a.Key}={Describe(a.Value)}"))));
            }

            await StoreScenario.RunAsync("os_dynamo",
                builder => builder.UseDynamoDb(options => options.ClientConfig = config),
                ReadAsync,
                expectedAfterCopy:
                    """
                    b=true big=x*3000 d='0102' id=1 n=1.5 tags='["a","b c"]' v='one'
                    b=false big=NULL d=NULL id=2 n=NULL tags=NULL v='two'
                    """,
                expected:
                    """
                    b=true big=x*3000 d='0102' id=1 n=1.5 tags='["a","b c"]' v='uno'
                    b=false big=NULL d=NULL id=2 n=2.25 tags='["x"]' v=NULL
                    b=NULL big=NULL d='ff' id=30 n=3 tags='[]' v='three'
                    """,
                expectedKeyLast:
                    """
                    id=1 v='changed'
                    id=2 v='new'
                    """,
                expectedCompositeKey:
                    """
                    SK='x' code='x' id=1 v='changed'
                    SK='zz' code='zz' id=3 v='new'
                    """);
        }
    }
}
