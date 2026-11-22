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

        private readonly bool _useDeduplication;

        private AmazonDynamoDBClient? _client;

        // Table schemas stored in memory
        private readonly Dictionary<string, List<ColumnInfo>> _tableColumns = [];

        // the items of the next batch write (25 at most, the limit of DynamoDB)
        private readonly List<(string TableName, WriteRequest WriteRequest)> _batch = new(25);

        // the items of the next batch write and of those in flight: a second change of one of them waits for them
        private readonly HashSet<string> _batchKeys = [];

        private readonly List<Task> _inFlight = [];

        private readonly Utf8Buffer _utf8 = new();

        private string _tableName = "";
        private string _schemaName = "";
        private string _relationName = "";

        // the DynamoDB table of each PostgreSQL table (GetTableName), shared by the publishers of the factory
        internal TargetNames Names { get; init; }

        /// <summary>The DynamoDB table of the table of the current message.</summary>
        private string DynamoTableName() => Names.Get(_tableName, _schemaName, _relationName);

        // the values of the row and of the key of the change being written
        private JsonElement[] _row = [];
        private JsonElement[] _key = [];

        /// <summary>
        /// The sort key attribute of a table with more than one key column: the other key values, joined with "|".
        /// </summary>
        private const string SortKeyName = "SK";

        private ulong? _lastWal;
        private ulong _lastMessageNo;

        public DynamoDbPublisher(DynamoDbPublisherOptions options, ILogger<DynamoDbPublisher>? logger, bool useDeduplication = true)
        {
            _options = options;
            Names = new TargetNames(options.GetTableName);
            _logger = logger;
            _useDeduplication = useDeduplication;
        }

        public override async Task PublishAsync(JsonMessage msg, CancellationToken token)
        {
            var client = await EnsureClientAsync(token).ConfigureAwait(false);

            _tableName = msg.TableName;
            _schemaName = msg.SchemaName;
            _relationName = msg.RelationName;

            var tableName = _tableName;

            using var doc = JsonDocument.Parse(_utf8.Encode(msg.Json));

            await TryParseSchemaAsync(client, tableName, doc, token).ConfigureAwait(false);

            await ParseRowAsync(client, tableName, doc, token).ConfigureAwait(false);

            // the messages of a table sync are behind the main stream - they must not move the saved position
            if (!msg.IsTableSync)
            {
                _lastWal = msg.TxFinalLsn;
                _lastMessageNo = msg.MessageNo;
            }
        }

        public override async Task ConfirmAsync(CancellationToken token)
        {
            var client = await EnsureClientAsync(token).ConfigureAwait(false);

            // every item first, then the position: it is never ahead of them
            await WriteAllAsync(client, token).ConfigureAwait(false);

            // _lastWal has no value only if nothing has been published since the last confirm (or ever);
            // it may still need persisting here even with an empty _batch, e.g. a transaction with only
            // logical decoding messages.
            // Data exporter messages have no LSN info (0,0) - they must not overwrite the replication position
            if (_useDeduplication && _lastWal.HasValue && (_lastWal.Value != 0 || _lastMessageNo != 0))
            {
                await client.SaveConfigAsync(ConfigKey.Position, DynamoDbExtensions.FormatPosition(_lastWal.Value, _lastMessageNo), token)
                    .ConfigureAwait(false);
            }
        }

        public override async Task<(ulong, ulong)> GetLastPublishedWalSeqAsync(CancellationToken token)
        {
            // without deduplication there is no need to read the last published position
            if (!_useDeduplication) return (0UL, 0UL);

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
            doc.RootElement.TryGetProperty("c", out var changeTypeElement);

            var changeType = changeTypeElement.GetString();

            if (changeType == "M") return; // logical decoding message - carries no table and no row

            if (tableName.Length == 0) throw new Exception($"Missing table name for change type '{changeType}'");

            if (!_tableColumns.TryGetValue(tableName, out var columns))
            {
                columns = await client.GetSchemaAsync(tableName, token)
                    .ConfigureAwait(false);

                if (columns == null) throw new Exception($"Missing table schema: {tableName}");

                _tableColumns[tableName] = columns;
            }

            // read once: indexing a JSON array walks it from the start when it has arrays or objects in it
            var keyElement = doc.RootElement.TryGetProperty("k", out var key) && key.ValueKind == JsonValueKind.Array ? ReadValues(key, ref _key) : null;
            var rowElement = doc.RootElement.TryGetProperty("r", out var row) && row.ValueKind == JsonValueKind.Array ? ReadValues(row, ref _row) : [];

            if (keyElement != null)
            {
                // Key element is present only for deletes or if the PK has changed (or if replica identity is all).
                // Changing PK is not supported - we must delete the old, and create a new item.

                var keyAttributes = ComposeKeyAttributes(columns, keyElement, isKeyRow: true);

                var request = new WriteRequest
                {
                    DeleteRequest = new DeleteRequest { Key = keyAttributes }
                };

                await AddToBatchAsync(client, DynamoTableName(), request, keyAttributes, token)
                    .ConfigureAwait(false);
            }

            if (changeType != "D") // a delete has no row, it is handled above with the key
            {
                var itemAttributes = ComposeItemAttributes(columns, rowElement, out var hasToastedValues);

                // if we have toasted values we must update, which is not supported in a batch
                if (hasToastedValues)
                {
                    // the items before it are written first
                    await WriteAllAsync(client, token)
                        .ConfigureAwait(false);

                    var keyAttributes = ComposeKeyAttributes(columns, rowElement, isKeyRow: false);

                    // then update the item - the key attributes identify it and cannot be set
                    foreach (var keyAttribute in keyAttributes.Keys)
                    {
                        itemAttributes.Remove(keyAttribute);
                    }

                    if (itemAttributes.Count == 0) return;

                    var request = BuildUpdateItemRequest(DynamoTableName(), keyAttributes, itemAttributes);

                    await client.UpdateItemAsync(request, token)
                        .ConfigureAwait(false);
                }
                else
                {
                    // the sort key is not a column, so it has to be added to the item
                    var keyAttributes = ComposeKeyAttributes(columns, rowElement, isKeyRow: false);

                    foreach (var keyAttribute in keyAttributes)
                    {
                        itemAttributes[keyAttribute.Key] = keyAttribute.Value;
                    }

                    var request = new WriteRequest
                    {
                        PutRequest = new PutRequest { Item = itemAttributes },
                    };

                    await AddToBatchAsync(client, DynamoTableName(), request, keyAttributes, token)
                        .ConfigureAwait(false);
                }
            }
        }

        private async Task AddToBatchAsync(AmazonDynamoDBClient client, string tableName, WriteRequest request, Dictionary<string, AttributeValue> key, CancellationToken token)
        {
            var batchKey = tableName + "|" + string.Join("|", key.OrderBy(a => a.Key, StringComparer.Ordinal).Select(a => $"{a.Key}={a.Value.S ?? a.Value.N ?? a.Value.BOOL?.ToString()}"));

            // a second change of an item waits until its first one is written: a batch write must not have an item twice,
            // and the writes in flight can end in any order
            if (_batchKeys.Contains(batchKey))
            {
                await WriteAllAsync(client, token)
                    .ConfigureAwait(false);
            }

            _batch.Add((tableName, request));
            _batchKeys.Add(batchKey);

            // DynamoDB takes 25 items in a batch write
            if (_batch.Count >= 25)
            {
                await SendBatchAsync(client, token)
                    .ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Sends the items of the next batch write, without waiting for it unless MaxParallelBatchWrites are in flight.
        /// </summary>
        private async Task SendBatchAsync(AmazonDynamoDBClient client, CancellationToken token)
        {
            if (_batch.Count == 0) return;

            var batchRequest = new BatchWriteItemRequest { RequestItems = [] };

            foreach (var (tableName, req) in _batch)
            {
                if (!batchRequest.RequestItems.TryGetValue(tableName, out var list))
                {
                    list = batchRequest.RequestItems[tableName] = [];
                }

                list.Add(req);
            }

            _batch.Clear();

            _inFlight.Add(WriteBatchAsync(client, batchRequest, token));

            if (_inFlight.Count >= Math.Max(1, _options.MaxParallelBatchWrites))
            {
                // the oldest one: its items stay in _batchKeys until all of them are written
                var oldest = _inFlight[0];

                _inFlight.RemoveAt(0);

                await oldest.ConfigureAwait(false);
            }
        }

        private static async Task WriteBatchAsync(AmazonDynamoDBClient client, BatchWriteItemRequest batchRequest, CancellationToken token)
        {
            var response = await client.BatchWriteItemAsync(batchRequest, token)
                .ConfigureAwait(false);

            if (response.UnprocessedItems?.Count > 0)
            {
                throw new Exception($"Some items {response.UnprocessedItems.Count} were unprocessed in batch write.");
            }
        }

        /// <summary>
        /// Writes every item: the next batch write, and waits for all of them in flight.
        /// </summary>
        private async Task WriteAllAsync(AmazonDynamoDBClient client, CancellationToken token)
        {
            await SendBatchAsync(client, token).ConfigureAwait(false);

            try
            {
                await Task.WhenAll(_inFlight).ConfigureAwait(false);
            }
            finally
            {
                _inFlight.Clear();
            }

            _batchKeys.Clear();
        }

        /// <summary>
        /// The values of a JSON array, in an array of exactly their number, reused while that number stays.
        /// </summary>
        private static JsonElement[] ReadValues(JsonElement array, ref JsonElement[] values)
        {
            var length = array.GetArrayLength();

            if (values.Length != length) values = new JsonElement[length];

            var i = 0;

            foreach (var value in array.EnumerateArray()) values[i++] = value;

            return values;
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

        /// <summary>
        /// The key of an item: the first key column is the partition key, the other key columns are joined
        /// into the sort key. A key row ("k") has the key columns only, a row ("r") has all the columns.
        /// </summary>
        private static Dictionary<string, AttributeValue> ComposeKeyAttributes(List<ColumnInfo> columns, JsonElement[] element, bool isKeyRow)
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
                    if (!isKeyRow) i++;
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
                    if (string.IsNullOrEmpty(sortKeyName)) sortKeyName = SortKeyName;

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

        private static Dictionary<string, AttributeValue> ComposeItemAttributes(List<ColumnInfo> columns, JsonElement[] rowElement, out bool hasToastedValues)
        {
            hasToastedValues = false;

            var attrs = new Dictionary<string, AttributeValue>();

            var i = 0;
            foreach (var col in columns)
            {
                if (rowElement[i].ValueKind == JsonValueKind.String && rowElement[i].ValueEquals("__TOAST__"))
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
                    // the text of the number, as PostgreSQL wrote it: a double would lose the digits of a numeric
                    // beyond about 15, and DynamoDB keeps 38
                    return new AttributeValue { N = element.GetRawText() };
                case JsonValueKind.True:
                case JsonValueKind.False:
                    return new AttributeValue { BOOL = element.GetBoolean() };
                case JsonValueKind.Null:
                case JsonValueKind.Undefined:
                    return new AttributeValue { NULL = true };
                case JsonValueKind.Array:
                    // an array column is stored as its JSON text
                    return new AttributeValue { S = element.GetRawText() };
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

            await CreateTableIfNotExistsAsync(client, tableName, DynamoTableName(), columns, _options, token)
                .ConfigureAwait(false);

            await client.SetSchemaAsync(tableName, columns, token)
                .ConfigureAwait(false);
        }

        // Create the table with composite key (joined keys with "|"); the billing mode is by the name of the PostgreSQL table
        public static async Task CreateTableIfNotExistsAsync(AmazonDynamoDBClient client,
                                                             string tableName,
                                                             string dynamoTableName,
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
                keySchema.Add(new() { AttributeName = SortKeyName, KeyType = KeyType.RANGE });
                attributeDefinitions.Add(new() { AttributeName = SortKeyName, AttributeType = ScalarAttributeType.S });
            }

            var request = new CreateTableRequest
            {
                TableName = dynamoTableName,
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

