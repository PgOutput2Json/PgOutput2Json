using System.Collections.Generic;
using System.Globalization;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace PgOutput2Json.DynamoDb
{
    internal static class ConfigKey
    {
        public const string WalEnd = "WalEnd";
        public const string Schema = "Schema";

        public const string ConfigTable = "__pg2j_config";
    }

    internal static class DynamoDbExtensions
    {
        public static async Task<ulong> GetWalEndAsync(this AmazonDynamoDBClient client, CancellationToken token)
        {
            var val = await client.GetConfigAsync(ConfigKey.WalEnd, token).ConfigureAwait(false);
            return val != null ? ulong.Parse(val, CultureInfo.InvariantCulture) : 0;
        }

        public static async Task SetSchemaAsync(this AmazonDynamoDBClient client, string tableName, IReadOnlyList<ColumnInfo> cols, CancellationToken token)
        {
            var json = JsonSerializer.Serialize(cols, JsonContext.Default.ListColumnInfo);
            await client.SaveConfigAsync($"{ConfigKey.Schema}_{tableName}", json, token).ConfigureAwait(false);
        }

        public static async Task<List<ColumnInfo>?> GetSchemaAsync(this AmazonDynamoDBClient client, string tableName, CancellationToken token)
        {
            var val = await client.GetConfigAsync($"{ConfigKey.Schema}_{tableName}", token).ConfigureAwait(false);
            if (val == null) return null;

            return JsonSerializer.Deserialize(val, JsonContext.Default.ListColumnInfo);
        }

        public static async Task SaveConfigAsync(this AmazonDynamoDBClient client, string key, string value, CancellationToken token)
        {
            var item = new Dictionary<string, AttributeValue>
            {
                ["ConfigKey"] = new AttributeValue { S = key },
                ["Value"] = new AttributeValue { S = value }
            };

            var request = new PutItemRequest
            {
                TableName = ConfigKey.ConfigTable,
                Item = item
            };

            await client.PutItemAsync(request, token).ConfigureAwait(false);
        }

        public static async Task<string?> GetConfigAsync(this AmazonDynamoDBClient client, string key, CancellationToken token)
        {
            var request = new GetItemRequest
            {
                TableName = ConfigKey.ConfigTable,
                Key = new Dictionary<string, AttributeValue>
                {
                    ["ConfigKey"] = new AttributeValue { S = key }
                }
            };

            var response = await client.GetItemAsync(request, token).ConfigureAwait(false);
            if (response.Item == null || !response.Item.TryGetValue("Value", out var val)) return null;

            return val.S;
        }

        public static async Task TryCreateConfigTableAsync(this AmazonDynamoDBClient client, DynamoDbPublisherOptions options, CancellationToken token)
        {
            List<KeySchemaElement> keySchema = [new () { AttributeName = "ConfigKey", KeyType = KeyType.HASH }];
            List<AttributeDefinition> attributeDefinitions = [new() { AttributeName = "ConfigKey", AttributeType = ScalarAttributeType.S }];

            var request = new CreateTableRequest
            {
                TableName = ConfigKey.ConfigTable,
                KeySchema = keySchema,
                AttributeDefinitions = attributeDefinitions,
                BillingMode = options.TableBillingModes.TryGetValue(ConfigKey.ConfigTable, out var billingMode)
                    ? billingMode
                    : options.DefaultBillingMode,
            };

            try
            {
                await client.CreateTableAsync(request, token).ConfigureAwait(false);
            }
            catch (ResourceInUseException)
            {
                // Already exists
            }
        }
    }

    public struct ColumnInfo
    {
        public string Name { get; set; }
        public bool IsKey { get; set; }
        public uint DataType { get; set; }
        public int TypeModifier { get; set; }
    }
}

