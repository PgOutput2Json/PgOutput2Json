using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;
using MongoDB.Driver;

namespace PgOutput2Json.MongoDb
{
    public class MongoDbPublisher : MessagePublisher
    {
        private readonly MongoDbPublisherOptions _options;
        private readonly ILogger<MongoDbPublisher>? _logger;

        private readonly bool _useDeduplication;

        private MongoClient? _client;
        private IMongoDatabase? _db;

        private readonly Dictionary<string, List<ColumnInfo>> _tableColumns = [];

        private readonly List<BulkWriteModel> _batch = new(1000);

        private readonly Utf8Buffer _utf8 = new();

        private string _tableName = "";

        private readonly Dictionary<string, string> _namespaces = [];

        // the values of the row and of the key of the change being written
        private JsonElement[] _row = [];
        private JsonElement[] _key = [];

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
        private ulong? _lastWal;
        private ulong _lastMessageNo;

        public MongoDbPublisher(MongoDbPublisherOptions options, ILogger<MongoDbPublisher>? logger, bool useDeduplication = true)
        {
            _options = options;
            _logger = logger;
            _useDeduplication = useDeduplication;
        }

        public override async Task PublishAsync(JsonMessage msg, CancellationToken token)
        {
            var client = await EnsureDatabaseAsync(token).ConfigureAwait(false);

            // the rows of a batch are mostly of one table: its name is not made again for each of them
            if (!msg.TableName.Equals(_tableName.AsSpan())) _tableName = msg.TableName.ToString();

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
            var db = await EnsureDatabaseAsync(token).ConfigureAwait(false);

            // data exporter messages have no LSN info (0,0) - they must not overwrite the replication position;
            // without deduplication the watermark is not persisted at all
            var walEnd = _useDeduplication && _lastWal.HasValue && (_lastWal.Value != 0 || _lastMessageNo != 0)
                ? _lastWal
                : null;

            await db.ConfirmBatchAsync(walEnd, _lastMessageNo, _batch, token).ConfigureAwait(false);

            _batch.Clear();
        }

        public override async Task<(ulong, ulong)> GetLastPublishedWalSeqAsync(CancellationToken token)
        {
            // without deduplication there is no need to read the last published position
            if (!_useDeduplication) return (0UL, 0UL);

            var db = await EnsureDatabaseAsync(token).ConfigureAwait(false);

            return await db.GetWalEndAsync(token).ConfigureAwait(false);
        }

        public override ValueTask DisposeAsync()
        {
            _db = null;

            if (_client != null)
            {
                try
                {
                    _client.Dispose();
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Failed disposing MongoDB client");
                }
            }

            _client = null;

            return ValueTask.CompletedTask;
        }

        private async Task ParseRowAsync(IMongoDatabase db, string tableName, JsonDocument doc, CancellationToken token)
        {
            doc.RootElement.TryGetProperty("c", out var changeTypeElement);

            var changeType = changeTypeElement.GetString();

            if (changeType == "M") return; // logical decoding message - carries no table and no row

            if (tableName.Length == 0) throw new Exception($"Missing table name for change type '{changeType}'");

            if (!_tableColumns.TryGetValue(tableName, out var columns))
            {
                columns = await db.GetSchemaAsync(tableName, token).ConfigureAwait(false);

                if (columns != null)
                {
                    _tableColumns[tableName] = columns;
                }
            }

            if (columns == null) throw new Exception("Missing table schema: " + tableName);

            // read once: indexing a JSON array walks it from the start when it has arrays or objects in it
            var keyElement = doc.RootElement.TryGetProperty("k", out var key) && key.ValueKind == JsonValueKind.Array ? ReadValues(key, ref _key) : null;
            var rowElement = doc.RootElement.TryGetProperty("r", out var row) && row.ValueKind == JsonValueKind.Array ? ReadValues(row, ref _row) : [];

            if (!_namespaces.TryGetValue(tableName, out var collectionNamespace))
            {
                _namespaces[tableName] = collectionNamespace = db.GetCollectionNamespace(tableName);
            }

            MongoDbExtensions.UpsertOrDelete(_batch, collectionNamespace, columns, changeType, keyElement, rowElement);
        }

        private async Task TryParseSchemaAsync(IMongoDatabase db, string tableName, JsonDocument doc, CancellationToken token)
        {
            if (!doc.RootElement.TryGetProperty("s", out var schemaElement)) return;

            if (schemaElement.ValueKind != JsonValueKind.Array) throw new Exception("Invalid schema - expected array");

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

                    if (colLength < 4 || !colElement[3].TryGetInt32(out var typeModifier))
                    {
                        typeModifier = -1;
                    }

                    var typeName = colLength > 4 ? colElement[4].GetString() : null;

                    columns.Add(new ColumnInfo { Name = name, DataType = dataType, TypeModifier = typeModifier, TypeName = typeName, IsKey = isKey == 1 });
                }
            }

            _tableColumns[tableName] = columns;

            await db.EnsureUniqueKeyIndexAsync(tableName, columns, token).ConfigureAwait(false);

            await db.SetSchemaAsync(tableName, columns, token).ConfigureAwait(false);
        }

        private async Task<IMongoClient> EnsureClientAsync(CancellationToken token)
        {
            if (_client != null) return _client;

            _client = new MongoClient(_options.ClientSettings);

            if (_options.PostConnectionSetup != null)
            {
                await _options.PostConnectionSetup(_client).ConfigureAwait(false);
            }

            return _client;
        }

        private async Task<IMongoDatabase> EnsureDatabaseAsync(CancellationToken token)
        {
            if (_db != null) return _db;

            var client = await EnsureClientAsync(token).ConfigureAwait(false);

            _db = client.GetDatabase(_options.DatabaseName, new MongoDatabaseSettings
            {
                WriteConcern = WriteConcern.WMajority,
                ReadConcern = ReadConcern.Majority,
            });

            return _db;
        }
    }
}
