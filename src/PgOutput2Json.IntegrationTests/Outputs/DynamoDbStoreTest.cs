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
            if (value.N != null) return value.N;
            if (value.B != null) return "x" + Convert.ToHexString(value.B.ToArray());
            if (value.S != null) return value.S.Length > 100 ? $"{value.S[0]}*{value.S.Length}" : $"'{value.S}'";

            return "?";
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
                    items = (await client.ScanAsync(new ScanRequest { TableName = $"public.{table}", ConsistentRead = true })).Items;
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
