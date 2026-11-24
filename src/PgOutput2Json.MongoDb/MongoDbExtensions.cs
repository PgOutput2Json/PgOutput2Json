using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using MongoDB.Bson;
using MongoDB.Driver;

namespace PgOutput2Json.MongoDb
{
    internal static class MongoDbExtensions
    {
        private const string ToastValue = "__TOAST__";

        public static async Task<(ulong, ulong)> GetWalEndAsync(this IMongoDatabase db, string subscriptionName, CancellationToken token)
        {
            var position = await db.GetConfigAsync(ConfigKey.PositionOf(subscriptionName), token).ConfigureAwait(false);

            if (position != null) return ParsePosition(position);

            // before version 2.2 the store had one position: the first subscription takes it. The old keys are removed first,
            // so that no other subscription takes it too; a stop in between only loses it, and the changes are written again
            var oldPosition = await db.GetConfigAsync(ConfigKey.Position, token).ConfigureAwait(false);
            var walEndValue = await db.GetConfigAsync(ConfigKey.WalEnd, token).ConfigureAwait(false);
            var messageNoValue = await db.GetConfigAsync(ConfigKey.MessageNo, token).ConfigureAwait(false);

            if (oldPosition == null && walEndValue == null && messageNoValue == null) return (0, 0);

            var walEnd = oldPosition != null
                ? ParsePosition(oldPosition)
                : (walEndValue != null ? ulong.Parse(walEndValue, CultureInfo.InvariantCulture) : 0,
                   messageNoValue != null ? ulong.Parse(messageNoValue, CultureInfo.InvariantCulture) : 0);

            await db.DeleteConfigAsync([ConfigKey.Position, ConfigKey.WalEnd, ConfigKey.MessageNo], token).ConfigureAwait(false);
            await db.SaveConfigAsync(ConfigKey.PositionOf(subscriptionName), FormatPosition(walEnd.Item1, walEnd.Item2), token).ConfigureAwait(false);

            return walEnd;
        }

        internal static string FormatPosition(ulong walEnd, ulong messageNo)
        {
            return string.Create(CultureInfo.InvariantCulture, $"{walEnd}/{messageNo}");
        }

        internal static (ulong, ulong) ParsePosition(string position)
        {
            var slash = position.IndexOf('/');

            return (ulong.Parse(position.AsSpan(0, slash), CultureInfo.InvariantCulture),
                    ulong.Parse(position.AsSpan(slash + 1), CultureInfo.InvariantCulture));
        }

        public static async Task SetSchemaAsync(this IMongoDatabase db, string tableName, IReadOnlyList<ColumnInfo> cols, CancellationToken token)
        {
#pragma warning disable VSTHRD103 // Call async methods when in an async method
            var json = JsonSerializer.Serialize(cols, JsonContext.Default.ListColumnInfo);
#pragma warning disable VSTHRD103 // Call async methods when in an async method
            await db.SaveConfigAsync($"{ConfigKey.Schema}_{tableName}", json, token).ConfigureAwait(false);
        }

        public static async Task<List<ColumnInfo>?> GetSchemaAsync(this IMongoDatabase db, string tableName, CancellationToken token)
        {
            var cfgValue = await db.GetConfigAsync($"{ConfigKey.Schema}_{tableName}", token).ConfigureAwait(false);

            if (cfgValue == null) return null;

            return JsonSerializer.Deserialize(cfgValue, JsonContext.Default.ListColumnInfo);
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

        public static async Task DeleteConfigAsync(this IMongoDatabase db, IEnumerable<string> keys, CancellationToken token)
        {
            var collection = db.GetCollection<Config>("__pg2j_config");

            await collection.DeleteManyAsync(Builders<Config>.Filter.In(cfg => cfg.Id, keys), token).ConfigureAwait(false);
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

        /// <summary>
        /// The namespace of a collection in the database (database.collection).
        /// </summary>
        public static string GetCollectionNamespace(this IMongoDatabase db, string collectionName)
        {
            return $"{db.DatabaseNamespace.DatabaseName}.{collectionName}";
        }

        public static void UpsertOrDelete(List<BulkWriteModel> batch,
                                          string collectionNamespace,
                                          IReadOnlyList<ColumnInfo> columns,
                                          string? changeType,
                                          JsonElement[]? keyElement,
                                          JsonElement[] rowElement,
                                          TimeZoneInfo timestampTimeZone)
        {
            var model = changeType == "D"
                ? Delete(collectionNamespace, columns, keyElement, timestampTimeZone)
                : Upsert(collectionNamespace, columns, keyElement, rowElement, timestampTimeZone);

            if (model != null)
            {
                batch.Add(model);
            }
        }

        public static async Task ConfirmBatchAsync(this IMongoDatabase db, string subscriptionName, ulong? walEnd, ulong messageNo, List<BulkWriteModel> batch, CancellationToken token)
        {
            if (walEnd.HasValue)
            {
                var configNamespace = $"{db.DatabaseNamespace.DatabaseName}.__pg2j_config";

                // the last write of the ordered bulk write: saved only once all the rows before it are
                batch.Add(new BulkWriteReplaceOneModel<Config>(
                    configNamespace,
                    Builders<Config>.Filter.Eq(cfg => cfg.Id, ConfigKey.PositionOf(subscriptionName)),
                    new Config { Id = ConfigKey.PositionOf(subscriptionName), Value = FormatPosition(walEnd.Value, messageNo) },
                    isUpsert: true
                ));
            }

            // batch may only contain the config upserts above (e.g. a transaction with only logical decoding messages)
            if (batch.Count == 0) return;

            await db.Client.BulkWriteAsync(batch, cancellationToken: token).ConfigureAwait(false);
        }

        /// <summary>
        /// A row with all its values replaces its document, or inserts it, which costs MongoDB less than setting every
        /// field. A row with a large value that an update did not change (TOAST) sets the other fields, and keeps that one.
        /// The filters and the documents are BsonDocuments: the builders of the driver are rendered again for every row.
        /// </summary>
        public static BulkWriteModel? Upsert(string collectionName, IReadOnlyList<ColumnInfo> columns, JsonElement[]? keyElement, JsonElement[] rowElement, TimeZoneInfo timestampTimeZone)
        {
            var filter = new BsonDocument();

            // an update of the key has the old key, and finds the document by it
            if (keyElement != null)
            {
                var keyIndex = 0;

                for (var i = 0; i < columns.Count; i++)
                {
                    if (!columns[i].IsKey) continue;

                    filter[columns[i].Name] = GetColumnValue(keyElement[keyIndex++], columns[i], timestampTimeZone);
                }
            }

            var hasFilter = filter.ElementCount > 0;

            var document = new BsonDocument();
            var hasToast = false;

            for (var i = 0; i < columns.Count; i++)
            {
                var column = columns[i];
                var value = rowElement[i];

                // the key is in the filter even when it is a large value that was not sent: never an empty filter
                if (column.IsKey && !hasFilter) filter[column.Name] = GetColumnValue(value, column, timestampTimeZone);

                if (value.ValueKind == JsonValueKind.String && value.ValueEquals(ToastValue))
                {
                    hasToast = true;
                    continue; // unchanged, so not sent
                }

                document[column.Name] = GetColumnValue(value, column, timestampTimeZone);
            }

            if (document.ElementCount == 0) return null;

            return hasToast
                ? new BulkWriteUpdateOneModel<BsonDocument>(collectionName, filter, new BsonDocument("$set", document), isUpsert: true)
                : new BulkWriteReplaceOneModel<BsonDocument>(collectionName, filter, document, isUpsert: true);
        }

        public static BulkWriteDeleteOneModel<BsonDocument>? Delete(string collectionName, IReadOnlyList<ColumnInfo> columns, JsonElement[]? keyElement, TimeZoneInfo timestampTimeZone)
        {
            if (keyElement == null) return null;

            var filter = new BsonDocument();

            var keyIndex = 0;

            for (var i = 0; i < columns.Count; i++)
            {
                if (!columns[i].IsKey) continue;

                filter[columns[i].Name] = GetColumnValue(keyElement[keyIndex++], columns[i], timestampTimeZone);
            }

            return new BulkWriteDeleteOneModel<BsonDocument>(collectionName, filter);
        }

        private static BsonValue GetColumnValue(JsonElement valElement, ColumnInfo column, TimeZoneInfo timestampTimeZone)
        {
            // the unique index of the key indexes each element of an array field, so two rows that share an element
            // would be duplicates: an array in a key column is stored as its JSON text
            if (column.IsKey && valElement.ValueKind == JsonValueKind.Array) return valElement.GetRawText();

            switch (valElement.ValueKind)
            {
                case JsonValueKind.String:
                    if (column.PgOid.IsTimestamp())
                    {
                        return ConvertTimestamp(valElement.GetString()!, timestampTimeZone);
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
                        return data != null ? ParseJson(data) : BsonNull.Value;
                    }
                    else if (column.IsTextArray)
                    {
                        var json = new StringBuilder();

                        JsonUtils.WriteArrayOfText(json, valElement.GetString());

                        // in a key column the JSON text, like the other arrays
                        return column.IsKey ? json.ToString() : ParseJson(json.ToString());
                    }
                    else
                    {
                        return valElement.GetString();
                    }
                case JsonValueKind.Array:
                    if (column.PgOid.IsArrayOfTimestamp())
                    {
                        return ConvertArray(valElement, v => ConvertTimestamp(v, timestampTimeZone));
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
                        return ConvertArray(valElement, ParseJson);
                    }
                    else
                    {
                        return ConvertJsonElementToBsonArray(valElement);
                    }
                default:
                    return ConvertJsonElementToBsonValue(valElement);
            }
        }

        /// <summary>
        /// A BSON date (UTC, milliseconds) from PostgreSQL's text of a timestamp. A <c>timestamptz</c> has its offset in
        /// the text (UTC in the replication session); a <c>timestamp</c> (without time zone) has none and is taken as a
        /// wall time of <paramref name="timeZone"/> (by default UTC, so the wall time is stored as it is). The time zone of
        /// the machine is never used. A value a BSON date cannot hold here (infinity, a date BC, a year after 9999) is
        /// stored as PostgreSQL's text.
        /// </summary>
        internal static BsonValue ConvertTimestamp(string text, TimeZoneInfo timeZone)
        {
            if (!PgTimestamp.TryGetUtcTicks(text, timeZone, out var utcTicks)) return new BsonString(text);

            // the milliseconds since 1970, rounded down also before it
            var ticks = utcTicks - DateTime.UnixEpoch.Ticks;

            return new BsonDateTime(Math.DivRem(ticks, TimeSpan.TicksPerMillisecond, out var rest) - (rest < 0 ? 1 : 0));
        }

        /// <summary>
        /// The value of a json column: an object, but also an array, a number, a string, a boolean or null.
        /// A key that is twice in an object keeps its last value, as in jsonb.
        /// </summary>
        private static BsonValue ParseJson(string json)
        {
            using var doc = JsonDocument.Parse(json);

            return ConvertJsonElementToBsonValue(doc.RootElement);
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
                    else if (element.TryGetDouble(out var doubleValue) && !double.IsInfinity(doubleValue))
                    {
                        return doubleValue;
                    }
                    else if (element.TryGetDecimal(out var decimalValue))
                    {
                        return decimalValue;
                    }
                    else
                    {
                        // too large for any number type of MongoDB
                        return element.GetRawText();
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
                                                           string collectionName,
                                                           IReadOnlyList<ColumnInfo> columns,
                                                           CancellationToken token = default)
        {
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

        /// <summary>
        /// The name of a type that is not built in, from the schema (for a domain the name of its base type).
        /// </summary>
        public string? TypeName { get; set; }

        /// <summary>
        /// An array of a type that is not built in (an enum, a user type, a domain over an array): PostgreSQL names
        /// an array type after its element type with an underscore, and the library sends its value as the PostgreSQL text.
        /// </summary>
        public readonly bool IsTextArray => TypeName != null && TypeName.StartsWith('_');

        public readonly PgOid PgOid { get {  return (PgOid)DataType; } }
    }
}
