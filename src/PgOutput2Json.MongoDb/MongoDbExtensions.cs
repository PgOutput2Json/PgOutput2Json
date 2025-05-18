using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using MongoDB.Bson;
using MongoDB.Driver;

namespace PgOutput2Json.MongoDb
{
    internal static class MongoDbExtensions
    {
        public static async Task<ulong> GetWalEndAsync(this IMongoDatabase db, CancellationToken token)
        {
            var cfgValue = await db.GetConfigAsync(ConfigKey.WalEnd, token).ConfigureAwait(false);

            return cfgValue != null ? ulong.Parse(cfgValue, CultureInfo.InvariantCulture) : 0;
        }

        public static async Task SetSchemaAsync(this IMongoDatabase db, string tableName, IReadOnlyList<ColumnInfo> cols, CancellationToken token)
        {
            await db.SaveConfigAsync($"{ConfigKey.Schema}_{tableName}", JsonSerializer.Serialize(cols), token).ConfigureAwait(false);
        }

        public static async Task<List<ColumnInfo>?> GetSchemaAsync(this IMongoDatabase db, string tableName, CancellationToken token)
        {
            var cfgValue = await db.GetConfigAsync($"{ConfigKey.Schema}_{tableName}", token).ConfigureAwait(false);

            if (cfgValue == null) return null;

            return JsonSerializer.Deserialize<List<ColumnInfo>>(cfgValue);
        }

        public static async Task SaveConfigAsync(this IMongoDatabase db, string key, string value, CancellationToken token)
        {
            var collection = db.GetCollection<Config>("__pg2j_config");

            var result = await collection.ReplaceOneAsync(
                cfg => cfg.Id == key,
                new Config { Id = key, Value = value },
                new ReplaceOptions<Config> { IsUpsert = true },
                token
            ).ConfigureAwait(false);
        }

        public static async Task<string?> GetConfigAsync(this IMongoDatabase db, string key, CancellationToken token)
        {
            var collection = db.GetCollection<Config>("__pg2j_config");

            var cfg = await collection
                .Find(cfg => cfg.Id == key)
                .FirstOrDefaultAsync(token)
                .ConfigureAwait(false);

            return cfg?.Value;
        }

        private static string GetCollectionName(string fullTableName)
        {
            var nameParts = fullTableName.Split('.');
            return nameParts.Length > 1 ? $"{nameParts[0]}__{nameParts[1]}" : nameParts[0];
        }

        public static void UpsertOrDelete(this IMongoDatabase db,
                                          List<BulkWriteModel> batch,
                                          string fullTableName,
                                          IReadOnlyList<ColumnInfo> columns,
                                          JsonElement changeTypeElement,
                                          JsonElement keyElement,
                                          JsonElement rowElement)
        {
            var collectionName = GetCollectionName(fullTableName);
            var collectionNamespace = $"{db.DatabaseNamespace.DatabaseName}.{collectionName}";

            var changeType = changeTypeElement.GetString();


            BulkWriteModel? model;

            if (changeType == "D")
            {
                model = Delete(collectionNamespace, columns, keyElement);
            }
            else
            {
                model = Upsert(collectionNamespace, columns, keyElement, rowElement);
            }

            if (model != null)
            {
                batch.Add(model);
            }
        }

        public static async Task ConfirmBatchAsync(this IMongoDatabase db, ulong? walEnd, List<BulkWriteModel> batch, CancellationToken token)
        {
            if (batch.Count == 0) return;

            if (walEnd.HasValue)
            {
                batch.Add(new BulkWriteReplaceOneModel<Config>(
                    $"{db.DatabaseNamespace.DatabaseName}.__pg2j_config",
                    Builders<Config>.Filter.Eq(cfg => cfg.Id, ConfigKey.WalEnd),
                    new Config { Id = ConfigKey.WalEnd, Value = walEnd.Value.ToString(CultureInfo.InvariantCulture) },
                    isUpsert: true
                ));
            }

            await db.Client.BulkWriteAsync(batch, cancellationToken: token).ConfigureAwait(false);
        }

        public static BulkWriteUpdateOneModel<BsonDocument>? Upsert(string collectionName, IReadOnlyList<ColumnInfo> columns, JsonElement keyElement, JsonElement rowElement)
        {
            var filters = new List<FilterDefinition<BsonDocument>>();
            var updates = new List<UpdateDefinition<BsonDocument>>();

            int i;

            if (keyElement.ValueKind != JsonValueKind.Undefined)
            {
                i = 0;
                foreach (var column in columns)
                {
                    if (!column.IsKey) continue;

                    filters.Add(Builders<BsonDocument>.Filter.Eq(column.Name, GetColumnValue(keyElement[i], column)));
                    i++;
                }
            }

            var hasFilters = filters.Count > 0;

            i = 0;
            foreach (var column in columns)
            {
                if (column.IsKey && !hasFilters)
                {
                    filters.Add(Builders<BsonDocument>.Filter.Eq(column.Name, GetColumnValue(rowElement[i], column)));
                    i++;

                    continue;
                }

                if (rowElement[i].ValueKind == JsonValueKind.String && rowElement[i].GetString() == "__TOAST__")
                {
                    i++;
                    continue; // skip unchanged toasted columns
                }

                updates.Add(Builders<BsonDocument>.Update.Set(column.Name, BsonValue.Create(GetColumnValue(rowElement[i], column))));
                i++;
            }

            if (updates.Count > 0)
            {
                return new BulkWriteUpdateOneModel<BsonDocument>(
                    collectionName,
                    Builders<BsonDocument>.Filter.And(filters),
                    Builders<BsonDocument>.Update.Combine(updates),
                    isUpsert: true
                );
            }

            return null;
        }

        public static BulkWriteDeleteOneModel<BsonDocument> Delete(string collectionName, IReadOnlyList<ColumnInfo> columns, JsonElement keyElement)
        {
            var filters = new List<FilterDefinition<BsonDocument>>();

            var i = 0;

            foreach (var column in columns)
            {
                if (!column.IsKey) continue;

                filters.Add(Builders<BsonDocument>.Filter.Eq(column.Name, GetColumnValue(keyElement[i], column)));
                i++;
            }

            return new BulkWriteDeleteOneModel<BsonDocument>(collectionName, Builders<BsonDocument>.Filter.And(filters));
        }

        private static BsonValue GetColumnValue(JsonElement valElement, ColumnInfo column)
        {
            switch (valElement.ValueKind)
            {
                case JsonValueKind.String:
                    if (column.PgOid.IsTimestamp())
                    {
                        var dateTime = DateTimeOffset.Parse(valElement.GetString() ?? "1970-01-01 00:00:00", CultureInfo.InvariantCulture);
                        return new BsonDateTime(dateTime.ToUnixTimeMilliseconds());
                    }
                    else if (column.PgOid.IsByte())
                    {
                        var data = valElement.GetString();
                        return data != null ? new BsonBinaryData(GetBinaryData(data)) : BsonNull.Value;
                    }
                    else if (column.PgOid.IsUuid())
                    {
                        var data = valElement.GetString();
                        return data != null ? new BsonBinaryData(Guid.Parse(data), GuidRepresentation.Standard) : BsonNull.Value;
                    }
                    else if (column.PgOid.IsJson())
                    {
                        var data = valElement.GetString();
                        return data != null ? BsonDocument.Parse(data) : BsonNull.Value;
                    }
                    else
                    {
                        return valElement.GetString();
                    }
                case JsonValueKind.Array:
                    if (column.PgOid.IsArrayOfTimestamp())
                    {
                        return ConvertArray(valElement, v => new BsonDateTime(DateTimeOffset.Parse(v, CultureInfo.InvariantCulture).ToUnixTimeMilliseconds()), "1970-01-01 00:00:00");
                    }
                    else if (column.PgOid.IsArrayOfByte())
                    {
                        return ConvertArray(valElement, v => new BsonBinaryData(GetBinaryData(v)));
                    }
                    else if (column.PgOid.IsArrayOfUuid())
                    {
                        return ConvertArray(valElement, v => new BsonBinaryData(Guid.Parse(v), GuidRepresentation.Standard));
                    }
                    else if (column.PgOid.IsArrayOfJson())
                    {
                        return ConvertArray(valElement, BsonDocument.Parse);
                    }
                    else
                    {
                        return ConvertJsonElementToBsonArray(valElement);
                    }
                default:
                    return ConvertJsonElementToBsonValue(valElement);
            }
        }

        private static BsonArray ConvertArray(JsonElement element, Func<string, BsonValue> converter, string? nullResult = null) 
        {
            var result = new BsonArray();

            foreach (var item in element.EnumerateArray())
            {
                var val = item.GetString() ?? nullResult;

                if (val == null)
                {
                    result.Add(BsonNull.Value);
                }
                else
                {
                    result.Add(converter(val));
                }
            }

            return result;
        }

        public static BsonArray ConvertJsonElementToBsonArray(JsonElement jsonArray)
        {
            if (jsonArray.ValueKind != JsonValueKind.Array)
                throw new ArgumentException("Expected JsonValueKind.Array", nameof(jsonArray));

            var bsonArray = new BsonArray();

            foreach (var element in jsonArray.EnumerateArray())
            {
                bsonArray.Add(ConvertJsonElementToBsonValue(element));
            }

            return bsonArray;
        }

        public static BsonValue ConvertJsonElementToBsonValue(JsonElement element)
        {
            switch (element.ValueKind)
            {
                case JsonValueKind.Undefined:
                case JsonValueKind.Null:
                    return BsonNull.Value;
                case JsonValueKind.String:
                    return new BsonString(element.GetString());
                case JsonValueKind.Number:
                    if (element.TryGetInt64(out var longValue))
                    {
                        if (longValue >= int.MinValue && longValue <= int.MaxValue)
                        {
                            return (int)longValue;
                        }

                        return longValue;
                    }
                    else if (element.TryGetSingle(out var floatValue))
                    {
                        return floatValue;
                    }
                    else if (element.TryGetDouble(out var doubleValue))
                    {
                        return doubleValue;
                    }
                    else
                    {
                        element.TryGetDecimal(out var decimalValue);
                        return decimalValue;
                    }
                case JsonValueKind.True:
                case JsonValueKind.False:
                    return new BsonBoolean(element.GetBoolean());
                case JsonValueKind.Object:
                    {
                        var obj = new BsonDocument();
                        foreach (var prop in element.EnumerateObject())
                        {
                            obj[prop.Name] = ConvertJsonElementToBsonValue(prop.Value);
                        }
                        return obj;
                    }
                case JsonValueKind.Array:
                    return ConvertJsonElementToBsonArray(element);
                default:
                    return element.GetRawText();
            }
        }

        private static byte[] GetBinaryData(string data)
        {
            var bytes = new byte[data.Length / 2];

            var j = 0;
            for (int i = 0; i < data.Length; i += 2)
            {
                byte b = Convert.ToByte(data.Substring(i, 2), 16);
                bytes[j++] = b;
            }

            return bytes;
        }

        public static async Task EnsureUniqueKeyIndexAsync(this IMongoDatabase db,
                                                           string fullTableName,
                                                           IReadOnlyList<ColumnInfo> columns,
                                                           CancellationToken token = default)
        {
            var collectionName = GetCollectionName(fullTableName);

            var collection = db.GetCollection<BsonDocument>(collectionName);

            var indexKeysDef = Builders<BsonDocument>.IndexKeys;
            IndexKeysDefinition<BsonDocument>? indexKeys = null;

            foreach (var column in columns)
            {
                if (!column.IsKey)
                    continue;

                indexKeys = indexKeys == null
                    ? indexKeysDef.Ascending(column.Name)
                    : indexKeys.Ascending(column.Name);
            }

            if (indexKeys == null)
            {
                // No key columns to index
                return;
            }

            var indexModel = new CreateIndexModel<BsonDocument>(
                indexKeys,
                new CreateIndexOptions { Unique = true, Name = $"{collectionName}_pk" }
            );

            await collection.Indexes.CreateOneAsync(indexModel, cancellationToken: token).ConfigureAwait(false);
        }

        private class Config
        {
            public required string Id { get; set; }
            public string? Value { get; set; }
        }
    }

    public struct ColumnInfo
    {
        public string Name { get; set; }
        public bool IsKey { get; set; }
        public uint DataType { get; set; }
        public int TypeModifier { get; set; }

        public readonly PgOid PgOid { get {  return (PgOid)DataType; } }
    }
}
