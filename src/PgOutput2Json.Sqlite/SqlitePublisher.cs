using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

using Microsoft.Data.Sqlite;
using System.Collections.Generic;

namespace PgOutput2Json.Sqlite
{
    public class SqlitePublisher : IMessagePublisher
    {
        private readonly SqlitePublisherOptions _options;
        private readonly ReplicationListenerOptions _listenerOptions;
        private readonly ILogger<SqlitePublisher>? _logger;

        private SqliteConnection? _connection;

        private readonly Dictionary<string, List<ColumnInfo>> _tableColumns = new();

        public SqlitePublisher(SqlitePublisherOptions options,
                               ReplicationListenerOptions listenerOptions,
                               ILogger<SqlitePublisher>? logger)
        {
            _options = options;
            _listenerOptions = listenerOptions;
            _logger = logger;
        }

        public async Task PublishAsync(ulong walSeqNo, string json, string tableName, string keyColumnValue, int partition, CancellationToken token)
        {
            var connection = await EnsureConnection(token).ConfigureAwait(false);

            using var doc = JsonDocument.Parse(json);

            await TryParseSchema(connection, tableName, doc, token).ConfigureAwait(false);

            await ParseRow(connection, tableName, doc, token).ConfigureAwait(false);
        }

        public Task ConfirmAsync(CancellationToken token)
        {
            return Task.CompletedTask;
        }

        public async Task<ulong> GetLastPublishedWalSeq(CancellationToken token)
        {
            var cn = await EnsureConnection(token).ConfigureAwait(false);

            return await cn.GetWalEnd(token).ConfigureAwait(false);
        }

        public async ValueTask DisposeAsync()
        {
            if (_connection != null)
            {
                try
                {
                    await _connection.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Failed disposing Sqlite connection");
                }
            }
        }

        private async Task ParseRow(SqliteConnection connection, string tableName, JsonDocument doc, CancellationToken token)
        {
            if (!_tableColumns.TryGetValue(tableName, out var columns)) throw new Exception("Missing table schema: " + tableName);

            if (!doc.RootElement.TryGetProperty("w", out var walEndElement)) throw new Exception("Invalid JSON - missing WAL end LSN");
            if (!walEndElement.TryGetUInt64(out var walEnd)) throw new Exception($"Invalid JSON - invalid WAL end LSN {walEndElement.GetRawText()}");

            doc.RootElement.TryGetProperty("c", out var changeTypeElement);
            doc.RootElement.TryGetProperty("k", out var keyElement);
            doc.RootElement.TryGetProperty("r", out var rowElement);

            await connection.UpdateOrInsert(walEnd, tableName, columns, changeTypeElement, keyElement, rowElement, token)
                .ConfigureAwait(false);
        }

        private async Task TryParseSchema(SqliteConnection connection, string tableName, JsonDocument doc, CancellationToken token)
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
                    colElement[2].TryGetInt32(out var dataType);

                    if (colLength < 4 || !colElement[3].TryGetInt32(out var typeModifier))
                    {
                        typeModifier = -1;
                    }

                    columns.Add(new ColumnInfo { Name = name, DataType = dataType, TypeModifier = typeModifier, IsKey = isKey == 1 });
                }
            }

            _tableColumns[tableName] = columns;

            await connection.TryCreateTable(tableName, columns, token).ConfigureAwait(false);
        }

        private async Task<SqliteConnection> EnsureConnection(CancellationToken token)
        {
            if (_connection != null) return _connection;

            _connection = new SqliteConnection(_options.ConnectionStringBuilder.ConnectionString);

            await _connection.OpenAsync(token).ConfigureAwait(false);

            await _connection.CreateConfigTable(token).ConfigureAwait(false);
            
            return _connection;
        }
    }
}
