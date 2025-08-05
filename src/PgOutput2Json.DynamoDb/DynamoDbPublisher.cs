using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Microsoft.Extensions.Logging;

namespace PgOutput2Json.DynamoDb
{
    public class DynamoDbPublisher : MessagePublisher
    {
        private readonly DynamoDbPublisherOptions _options;
        private readonly ILogger<DynamoDbPublisher>? _logger;

        private AmazonDynamoDBClient? _client;

        // Table schemas stored in memory
        private readonly Dictionary<string, List<ColumnInfo>> _tableColumns = [];

        // Batch write items buffer
        private readonly List<(string TableName, WriteRequest WriteRequest)> _batch = new(25); // DynamoDB batch write limit

        private ulong? _lastWal;

        public DynamoDbPublisher(DynamoDbPublisherOptions options, ILogger<DynamoDbPublisher>? logger)
        {
            _options = options;
            _logger = logger;
        }

        public override async Task PublishAsync(JsonMessage msg, CancellationToken token)
        {
            var client = await EnsureClientAsync(token).ConfigureAwait(false);

            var tableName = msg.TableName.ToString();

            using var doc = JsonDocument.Parse(msg.Json.ToString());

            await TryParseSchemaAsync(client, tableName, doc, token).ConfigureAwait(false);

            await ParseRowAsync(client, tableName, doc, token).ConfigureAwait(false);
        }

        public override async Task ConfirmAsync(CancellationToken token)
        {
            if (_batch.Count == 0) return;

            var client = await EnsureClientAsync(token).ConfigureAwait(false);

            var batchRequest = new BatchWriteItemRequest { RequestItems = [] };

            // convert batch list to dictionary per table name
            foreach (var (tableName, req) in _batch)
            {
                if (!batchRequest.RequestItems.TryGetValue(tableName, out var list))
                {
                    list = batchRequest.RequestItems[tableName] = [];
                }

                list.Add(req);
            }

            var response = await client.BatchWriteItemAsync(batchRequest, token)
                .ConfigureAwait(false);

            if (response.UnprocessedItems?.Count > 0)
            {
                throw new Exception($"Some items {response.UnprocessedItems.Count} were unprocessed in batch write.");
            }

            _batch.Clear();

            if (_lastWal.HasValue)
            {
                await client.SaveConfigAsync(ConfigKey.WalEnd, _lastWal.Value.ToString(CultureInfo.InvariantCulture), token)
                    .ConfigureAwait(false);
            }
        }

        public override async Task<ulong> GetLastPublishedWalSeqAsync(CancellationToken token)
        {
            var client = await EnsureClientAsync(token).ConfigureAwait(false);

            return await client.GetWalEndAsync(token).ConfigureAwait(false);
        }

        public override ValueTask DisposeAsync()
        {
            try
            {
                _client?.Dispose();
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed disposing DynamoDb client");
            }

            _client = null;

            return ValueTask.CompletedTask;
        }

        private async Task ParseRowAsync(AmazonDynamoDBClient client, string tableName, JsonDocument doc, CancellationToken token)
        {
            if (!_tableColumns.TryGetValue(tableName, out var columns))
            {
                columns = await client.GetSchemaAsync(tableName, token)
                    .ConfigureAwait(false);

                if (columns == null) throw new Exception($"Missing table schema: {tableName}");

                _tableColumns[tableName] = columns;
            }

            if (!doc.RootElement.TryGetProperty("w", out var walEndElement)) throw new Exception("Missing WAL end LSN");
            if (!walEndElement.TryGetUInt64(out var walEnd)) throw new Exception($"Invalid WAL end LSN {walEndElement.GetRawText()}");

            doc.RootElement.TryGetProperty("c", out var changeTypeElement);
            doc.RootElement.TryGetProperty("k", out var keyElement);
            doc.RootElement.TryGetProperty("r", out var rowElement);

            if (keyElement.ValueKind != JsonValueKind.Undefined)
            {
                // Key element is present only for deletes or if the PK has changed (or if replica identity is all).
                // Changing PK is not supported - we must delete the old, and create a new item.

                var keyAttributes = ComposeKeyAttributes(columns, keyElement);

                var request = new WriteRequest
                {
                    DeleteRequest = new DeleteRequest { Key = keyAttributes }
                };

                await AddToBatchAsync(tableName, request, token)
                    .ConfigureAwait(false);
            }

            var changeType = changeTypeElement.GetString();

            if (changeType != "d") // delete is andled above with keyElement handling
            {
                var itemAttributes = ComposeItemAttributes(columns, rowElement, out var hasToastedValues);

                // if we have toasted values we must update, which is not supported in a batch
                if (hasToastedValues)
                {
                    // flush the current batch
                    await ConfirmAsync(token)
                        .ConfigureAwait(false);

                    var keyAttributes = ComposeKeyAttributes(columns, rowElement);

                    // then update the item
                    var request = BuildUpdateItemRequest(tableName, keyAttributes, itemAttributes);

                    await client.UpdateItemAsync(request, token)
                        .ConfigureAwait(false);
                }
                else
                {
                    var request = new WriteRequest
                    {
                        PutRequest = new PutRequest { Item = itemAttributes },
                    };

                    await AddToBatchAsync(tableName, request, token)
                        .ConfigureAwait(false);
                }
            }

            _lastWal = walEnd;
        }

        private async Task AddToBatchAsync(string tableName, WriteRequest request, CancellationToken token)
        {
            _batch.Add((tableName, request));

            // DynamoDB batch write max 25 items, flush if exceeded

            if (_batch.Count >= 25)
            {
                await ConfirmAsync(token)
                    .ConfigureAwait(false);
            }
        }

        private static UpdateItemRequest BuildUpdateItemRequest(string tableName, Dictionary<string, AttributeValue> key, Dictionary<string, AttributeValue> attributesToUpdate)
        {
            var updateExpressions = new List<string>();
            var expressionAttributeValues = new Dictionary<string, AttributeValue>();
            var expressionAttributeNames = new Dictionary<string, string>();

            foreach (var kvp in attributesToUpdate)
            {
                var attrName = kvp.Key;
                var placeholderName = $"#{attrName}";
                var placeholderValue = $":{attrName}";

                updateExpressions.Add($"{placeholderName} = {placeholderValue}");
                expressionAttributeValues[placeholderValue] = kvp.Value;
                expressionAttributeNames[placeholderName] = attrName;
            }

            return new UpdateItemRequest
            {
                TableName = tableName,
                Key = key,
                UpdateExpression = "SET " + string.Join(", ", updateExpressions),
                ExpressionAttributeValues = expressionAttributeValues,
                ExpressionAttributeNames = expressionAttributeNames,
                ReturnValues = "UPDATED_NEW"
            };
        }

        private static Dictionary<string, AttributeValue> ComposeKeyAttributes(List<ColumnInfo> columns, JsonElement element)
        {
            var keyAttributes = new Dictionary<string, AttributeValue>();

            // Partition key: concat all IsKey columns with "|" (as requested)
            var partitionKeyName = string.Empty;
            AttributeValue? partitionKeyValue = null;

            // Sort key value will be concatenated from remaining IsKey columns (if any)
            var sortKeyName = string.Empty;
            var sortKeyValue = string.Empty;

            var isFirstKey = true;

            var i = 0;
            foreach (var col in columns)
            {
                if (!col.IsKey)
                {
                    i++;
                    continue;
                }

                var value = GetAttributeValueFromJsonElement(element[i], col);

                if (isFirstKey)
                {
                    partitionKeyName = col.Name;
                    partitionKeyValue = value;
                    isFirstKey = false;
                }
                else
                {
                    if (string.IsNullOrEmpty(sortKeyName)) sortKeyName = "SK";

                    sortKeyValue += (sortKeyValue.Length > 0 ? "|" : "") + GetStringFromJsonElement(element[i]);
                }

                i++;
            }

            if (partitionKeyValue == null) throw new Exception("No partition key defined in schema");

            keyAttributes[partitionKeyName] = partitionKeyValue;

            if (!string.IsNullOrEmpty(sortKeyName))
            {
                keyAttributes[sortKeyName] = new AttributeValue { S = sortKeyValue };
            }

            return keyAttributes;
        }

        private static Dictionary<string, AttributeValue> ComposeItemAttributes(List<ColumnInfo> columns, JsonElement rowElement, out bool hasToastedValues)
        {
            hasToastedValues = false;

            var attrs = new Dictionary<string, AttributeValue>();

            var i = 0;
            foreach (var col in columns)
            {
                if (rowElement[i].ValueKind == JsonValueKind.String && rowElement[i].GetString() == "__TOAST__")
                {
                    hasToastedValues = true;
                    i++;
                    continue;
                }
                
                attrs[col.Name] = GetAttributeValueFromJsonElement(rowElement[i], col);

                i++;
            }

            return attrs;
        }

        public static AttributeValue GetAttributeValueFromJsonElement(JsonElement element, ColumnInfo col)
        {
            switch (element.ValueKind)
            {
                case JsonValueKind.String:
                    return new AttributeValue { S = element.GetString() };
                case JsonValueKind.Number:
                    if (element.TryGetInt64(out var longVal))
                        return new AttributeValue { N = longVal.ToString() };
                    if (element.TryGetDouble(out var doubleVal))
                        return new AttributeValue { N = doubleVal.ToString(CultureInfo.InvariantCulture) };
                    break;
                case JsonValueKind.True:
                case JsonValueKind.False:
                    return new AttributeValue { BOOL = element.GetBoolean() };
                case JsonValueKind.Null:
                case JsonValueKind.Undefined:
                    return new AttributeValue { NULL = true };
            }

            return new AttributeValue { NULL = true };
        }

        private static ScalarAttributeType GetAttributeType(ColumnInfo col)
        {
            if (!Enum.IsDefined(typeof(PgOid), col.DataType)) return ScalarAttributeType.S;

            var pgOid = (PgOid)col.DataType;

            return pgOid switch
            {
                PgOid.BOOLOID => new ScalarAttributeType("BOOL"),
                PgOid.BYTEAOID => ScalarAttributeType.B, // binary
                PgOid.INT8OID => ScalarAttributeType.N,
                PgOid.INT2OID => ScalarAttributeType.N,
                PgOid.INT4OID => ScalarAttributeType.N,
                PgOid.OIDOID => ScalarAttributeType.N,
                PgOid.FLOAT4OID => ScalarAttributeType.N,
                PgOid.FLOAT8OID => ScalarAttributeType.N,
                PgOid.NUMERICOID => ScalarAttributeType.N,
                _ => ScalarAttributeType.S, // Default fallback for unknown types
            };
        }

        public static string? GetStringFromJsonElement(JsonElement element)
        {
            switch (element.ValueKind)
            {
                case JsonValueKind.String:
                    return element.GetString();
                case JsonValueKind.Number:
                    if (element.TryGetInt64(out var longVal)) return longVal.ToString();
                    if (element.TryGetDouble(out var doubleVal)) return doubleVal.ToString(CultureInfo.InvariantCulture);
                    break;
                case JsonValueKind.True:
                case JsonValueKind.False:
                    return element.GetBoolean().ToString();
                case JsonValueKind.Null:
                case JsonValueKind.Undefined:
                    return null;
            }

            return null;
        }

        private async Task TryParseSchemaAsync(AmazonDynamoDBClient client, string tableName, JsonDocument doc, CancellationToken token)
        {
            if (!doc.RootElement.TryGetProperty("s", out var schemaElement)) return;

            if (schemaElement.ValueKind != JsonValueKind.Array)
                throw new Exception("Invalid schema - expected array");

            var columns = new List<ColumnInfo>();

            var schemaLength = schemaElement.GetArrayLength();

            for (var i = 1; i < schemaLength; i++)
            {
                var colElement = schemaElement[i];

                var colLength = colElement.GetArrayLength();
                if (colLength >= 3)
                {
                    var name = colElement[0].GetString() ?? string.Empty;
                    colElement[1].TryGetByte(out var isKey);
                    colElement[2].TryGetUInt32(out var dataType);

                    int typeModifier = -1;
                    if (colLength >= 4)
                    {
                        colElement[3].TryGetInt32(out typeModifier);
                    }

                    columns.Add(new ColumnInfo
                    {
                        Name = name,
                        DataType = dataType,
                        TypeModifier = typeModifier,
                        IsKey = isKey == 1
                    });
                }
            }

            _tableColumns[tableName] = columns;

            await CreateTableIfNotExistsAsync(client, tableName, columns, _options, token)
                .ConfigureAwait(false);

            await client.SetSchemaAsync(tableName, columns, token)
                .ConfigureAwait(false);
        }

        // Create the table with composite key (joined keys with "|")
        public static async Task CreateTableIfNotExistsAsync(AmazonDynamoDBClient client,
                                                             string tableName,
                                                             IReadOnlyList<ColumnInfo> columns,
                                                             DynamoDbPublisherOptions options,
                                                             CancellationToken token)
        {
            var keyColumns = columns.Where(c => c.IsKey).ToList();
            if (keyColumns.Count == 0) throw new Exception("At least one key column required");

            List<KeySchemaElement> keySchema = [new () { AttributeName = keyColumns[0].Name, KeyType = KeyType.HASH }];
            List<AttributeDefinition> attributeDefinitions = [new() { AttributeName = keyColumns[0].Name, AttributeType = GetAttributeType(keyColumns[0]) }];

            if (keyColumns.Count > 1)
            {
                keySchema.Add(new() { AttributeName = "SC", KeyType = KeyType.RANGE });
                attributeDefinitions.Add(new() { AttributeName = "SC", AttributeType = ScalarAttributeType.S });
            }

            var request = new CreateTableRequest
            {
                TableName = tableName,
                KeySchema = keySchema,
                AttributeDefinitions = attributeDefinitions,
                BillingMode = options.TableBillingModes.TryGetValue(tableName, out var billingMode)
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

        private async Task<AmazonDynamoDBClient> EnsureClientAsync(CancellationToken token)
        {
            if (_client != null) return _client;

            _client = new AmazonDynamoDBClient(_options.ClientConfig);

            await _client.TryCreateConfigTableAsync(_options, token).ConfigureAwait(false);

            if (_options.PostConnectionSetup != null)
            {
                await _options.PostConnectionSetup(_client).ConfigureAwait(false);
            }

            return _client;
        }
    }
}

