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

        private MongoClient? _client;
        private IMongoDatabase? _db;

        private readonly Dictionary<string, List<ColumnInfo>> _tableColumns = [];

        private readonly List<BulkWriteModel> _batch = new(1000);
        private ulong? _lastWal;

        public MongoDbPublisher(MongoDbPublisherOptions options, ILogger<MongoDbPublisher>? logger)
        {
            _options = options;
            _logger = logger;
        }

        public override async Task PublishAsync(JsonMessage msg, CancellationToken token)
        {
            var client = await EnsureDatabaseAsync(token).ConfigureAwait(false);

            var tableName = msg.TableName.ToString();

            using var doc = JsonDocument.Parse(msg.Json.ToString());

            await TryParseSchemaAsync(client, tableName, msg.WalSeqNo, doc, token).ConfigureAwait(false);

            await ParseRowAsync(client, tableName, doc, token).ConfigureAwait(false);
        }

        public override async Task ConfirmAsync(CancellationToken token)
        {
            var db = await EnsureDatabaseAsync(token).ConfigureAwait(false);

            await db.ConfirmBatchAsync(_lastWal, _batch, token).ConfigureAwait(false);

            _batch.Clear();
        }

        public override async Task<ulong> GetLastPublishedWalSeqAsync(CancellationToken token)
        {
            var client = await EnsureDatabaseAsync(token).ConfigureAwait(false);

            return await client.GetWalEndAsync(token).ConfigureAwait(false);
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
            if (!_tableColumns.TryGetValue(tableName, out var columns))
            {
                columns = await db.GetSchemaAsync(tableName, token).ConfigureAwait(false);

                if (columns != null)
                {
                    _tableColumns[tableName] = columns;
                }
            }

            if (columns == null) throw new Exception("Missing table schema: " + tableName);

            if (!doc.RootElement.TryGetProperty("w", out var walEndElement)) throw new Exception("Invalid JSON - missing WAL end LSN");
            if (!walEndElement.TryGetUInt64(out var walEnd)) throw new Exception($"Invalid JSON - invalid WAL end LSN {walEndElement.GetRawText()}");

            doc.RootElement.TryGetProperty("c", out var changeTypeElement);
            doc.RootElement.TryGetProperty("k", out var keyElement);
            doc.RootElement.TryGetProperty("r", out var rowElement);

            db.UpsertOrDelete(_batch, tableName, columns, changeTypeElement, keyElement, rowElement);

            _lastWal = walEnd;
        }

        private async Task TryParseSchemaAsync(IMongoDatabase db, string tableName, ulong walSeq, JsonDocument doc, CancellationToken token)
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

                    columns.Add(new ColumnInfo { Name = name, DataType = dataType, TypeModifier = typeModifier, IsKey = isKey == 1 });
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
