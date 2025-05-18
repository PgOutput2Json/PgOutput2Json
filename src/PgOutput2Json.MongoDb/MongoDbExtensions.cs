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

        public static async Task SetWalEndAsync(this IMongoDatabase db, ulong walEnd, CancellationToken token)
        {
            await db.SaveConfigAsync(ConfigKey.WalEnd, walEnd.ToString(CultureInfo.InvariantCulture), token).ConfigureAwait(false);
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

        public static async Task UpsertOrDeleteAsync(this IMongoDatabase db,
                                                     ulong walEnd,
                                                     string fullTableName,
                                                     IReadOnlyList<ColumnInfo> columns,
                                                     JsonElement changeTypeElement,
                                                     JsonElement keyElement,
                                                     JsonElement rowElement,
                                                     CancellationToken token)
        {
            var collectionName = GetCollectionName(fullTableName);

            var collection = db.GetCollection<BsonDocument>(collectionName);

            var changeType = changeTypeElement.GetString();

            if (changeType == "D")
            {
                await DeleteAsync(collection, columns, keyElement, token).ConfigureAwait(false);
            }
            else
            {
                await UpsertAsync(collection, columns, keyElement, rowElement, token).ConfigureAwait(false);

            }

            await db.SetWalEndAsync(walEnd, token).ConfigureAwait(false);
        }

        public static async Task UpsertAsync(IMongoCollection<BsonDocument> collection, IReadOnlyList<ColumnInfo> columns, JsonElement keyElement, JsonElement rowElement, CancellationToken token)
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
                await collection.UpdateOneAsync(
                    Builders<BsonDocument>.Filter.And(filters),
                    Builders<BsonDocument>.Update.Combine(updates),
                    new UpdateOptions { IsUpsert = true },
                    token
                ).ConfigureAwait(false);
            }
        }

        public static async Task DeleteAsync(this IMongoCollection<BsonDocument> collection, IReadOnlyList<ColumnInfo> columns, JsonElement keyElement, CancellationToken token)
        {
            var filters = new List<FilterDefinition<BsonDocument>>();

            var i = 0;

            foreach (var column in columns)
            {
                if (!column.IsKey) continue;

                filters.Add(Builders<BsonDocument>.Filter.Eq(column.Name, GetColumnValue(keyElement[i], column)));
                i++;
            }

            await collection.DeleteOneAsync(Builders<BsonDocument>.Filter.And(filters), token).ConfigureAwait(false);
        }

        private static object? GetColumnValue(JsonElement valElement, ColumnInfo column)
        {
            switch (valElement.ValueKind)
            {
                case JsonValueKind.Undefined:
                case JsonValueKind.Null:
                    return null;
                case JsonValueKind.Number:
                    if (valElement.TryGetInt64(out var longValue))
                    {
                        if (longValue >= int.MinValue && longValue <= int.MaxValue)
                        {
                            return (int)longValue;
                        }

                        return longValue;
                    }
                    else
                    {
                        valElement.TryGetDecimal(out var decimalValue);
                        return decimalValue;
                    }
                case JsonValueKind.True:
                    return true;
                case JsonValueKind.False:
                    return false;
                case JsonValueKind.String:
                    if (column.IsDateTime())
                    {
                        var dateTime = DateTimeOffset.Parse(valElement.GetString() ?? "1970-01-01 00:00:00", CultureInfo.InvariantCulture);
                        return new BsonDateTime(dateTime.ToUnixTimeMilliseconds());
                    }
                    else if (column.IsBlob())
                    {
                        var data = valElement.GetString();

                        return data != null
                            ? (object)new BsonBinaryData(GetBinaryData(data))
                            : null;
                    }
                    else if (column.IsUuid())
                    {
                        var data = valElement.GetString();
                        return data != null ? new BsonBinaryData(Guid.Parse(data), GuidRepresentation.Standard) : null;
                    }
                    else
                    {
                        return valElement.GetString();
                    }
                default:
                    return valElement.GetRawText();
            }
        }

        private static byte[] GetBinaryData(string data)
        {
            var bytes = new byte[data.Length / 2];

            for (int i = 0; i < data.Length; i += 2)
            {
                byte b = Convert.ToByte(data.Substring(i, 2), 16);
                bytes[i] = b;
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

        public readonly bool IsDateTime()
        {
            return ((PgOid)DataType).IsTimestamp();
        }

        public readonly bool IsBlob()
        {
            return (PgOid)DataType == PgOid.BYTEAOID;
        }

        public readonly bool IsUuid()
        {
            return ((PgOid)DataType).IsUuid();
        }
    }
}
