using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;
using Microsoft.Data.Sqlite;

namespace PgOutput2Json.Sqlite
{
    public class SqlitePublisher : MessagePublisher
    {
        private readonly SqlitePublisherOptions _options;
        private readonly ILogger<SqlitePublisher>? _logger;

        private SqliteConnection? _connection;
        private DbTransaction? _transaction;

        private readonly Dictionary<string, TableContext> _tables = [];

        private ulong _lastWalEnd;
        private ulong _lastMessageNo;

        public SqlitePublisher(SqlitePublisherOptions options, ILogger<SqlitePublisher>? logger)
        {
            _options = options;
            _logger = logger;
        }

        public override async Task PublishAsync(JsonMessage msg, CancellationToken token)
        {
            var connection = await EnsureConnectionInTransactionAsync(token).ConfigureAwait(false);

            var tableName = msg.TableName.ToString();

            if (tableName.Length == 0) return; // logical decoding message (no table)

            var json = msg.Json.ToString();

            using var doc = JsonDocument.Parse(json);

            var table = await EnsureTableAsync(connection, tableName, doc, token).ConfigureAwait(false);

            doc.RootElement.TryGetProperty("c", out var changeTypeElement);
            doc.RootElement.TryGetProperty("k", out var keyElement);
            doc.RootElement.TryGetProperty("r", out var rowElement);

            var changeType = changeTypeElement.GetString();

            if (changeType == "I")
            {
                await connection.InsertAsync(table.Commands, rowElement, ignoreConflicts: true, token).ConfigureAwait(false);
            }
            else if (changeType == "U")
            {
                await UpdateRowAsync(connection, table, rowElement, token).ConfigureAwait(false);
            }
            else if (changeType == "D")
            {
                await connection.DeleteAsync(table.Commands, keyElement, token).ConfigureAwait(false);
            }

            _lastWalEnd = msg.TxFinalLsn;
            _lastMessageNo = msg.MessageNo;
        }

        public override async Task ConfirmAsync(CancellationToken token)
        {
            if (_transaction == null) return;

            try
            {
                // data exporter messages have no LSN info - they must not overwrite the replication position
                if (_connection != null && (_lastWalEnd != 0 || _lastMessageNo != 0))
                {
                    await _connection.SetWalEndAsync(_lastWalEnd, _lastMessageNo, token).ConfigureAwait(false);
                }

                await _transaction.CommitAsync(token).ConfigureAwait(false);
            }
            finally
            {
                _transaction = null;
                _lastWalEnd = 0;
                _lastMessageNo = 0;
            }

            if (_options.UseWal)
            {
                var cn = await EnsureConnectionAsync(token).ConfigureAwait(false);

                await cn.WalCheckpointAsync(_options.WalCheckpointType, _options.WalCheckpointTryCount, token).ConfigureAwait(false);
            }
        }

        public override async Task<(ulong, ulong)> GetLastPublishedWalSeqAsync(CancellationToken token)
        {
            var cn = await EnsureConnectionAsync(token).ConfigureAwait(false);

            return await cn.GetWalEndAsync(token).ConfigureAwait(false);
        }

        public override async ValueTask DisposeAsync()
        {
            foreach (var table in _tables.Values)
            {
                await table.DisposeAsync().ConfigureAwait(false);
            }

            _tables.Clear();

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

        private static async Task UpdateRowAsync(SqliteConnection connection, TableContext table, JsonElement rowElement, CancellationToken token)
        {
            var hasToast = HasUnchangedToast(rowElement);

            var updatedCount = hasToast
                ? await connection.FallbackUpdateAsync(table.Commands, table.FullTableName, rowElement, token).ConfigureAwait(false)
                : await connection.UpdateAsync(table.Commands, rowElement, token).ConfigureAwait(false);

            if (updatedCount == 0)
            {
                // row does not exist - insert it (same behavior as update-or-insert)
                await connection.InsertAsync(table.Commands, rowElement, ignoreConflicts: false, token).ConfigureAwait(false);
            }
        }

        private static bool HasUnchangedToast(JsonElement rowElement)
        {
            if (rowElement.ValueKind != JsonValueKind.Array) return false;

            foreach (var value in rowElement.EnumerateArray())
            {
                if (value.ValueKind == JsonValueKind.String && value.GetString() == "__TOAST__") return true;
            }

            return false;
        }

        private async Task<TableContext> EnsureTableAsync(SqliteConnection connection, string tableName, JsonDocument doc, CancellationToken token)
        {
            if (doc.RootElement.TryGetProperty("s", out var schemaElement))
            {
                // relation message - schema changed, rebuild the cached commands

                var columns = ParseSchema(schemaElement);

                var table = new TableContext(tableName, connection.CreatePreparedCommands(tableName, columns));

                if (_tables.Remove(tableName, out var existing))
                {
                    await existing.DisposeAsync().ConfigureAwait(false);
                }

                _tables[tableName] = table;

                await connection.CreateOrAlterTableAsync(tableName, columns, token).ConfigureAwait(false);

                await connection.SetSchemaAsync(tableName, columns, token).ConfigureAwait(false);

                return table;
            }

            if (_tables.TryGetValue(tableName, out var cached)) return cached;

            // schema not cached - try loading it from the config table

            var storedColumns = await connection.GetTableSchemaAsync(tableName, token).ConfigureAwait(false);

            if (storedColumns == null) throw new Exception("Missing table schema: " + tableName);

            var restored = new TableContext(tableName, connection.CreatePreparedCommands(tableName, storedColumns));

            _tables[tableName] = restored;

            return restored;
        }

        private static List<ColumnInfo> ParseSchema(JsonElement schemaElement)
        {
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

            return columns;
        }

        private async Task<SqliteConnection> EnsureConnectionAsync(CancellationToken token)
        {
            if (_connection != null) return _connection;

            _connection = new SqliteConnection(_options.ConnectionStringBuilder.ConnectionString);

            await _connection.OpenAsync(token).ConfigureAwait(false);

            if (_options.UseWal)
            {
                await _connection.UseWalAsync(token).ConfigureAwait(false);
            }

            await _connection.CreateConfigTableAsync(token).ConfigureAwait(false);

            if (_options.PostConnectionSetup != null)
            {
                await _options.PostConnectionSetup(_connection).ConfigureAwait(false);
            }

            return _connection;
        }

        private async Task<SqliteConnection> EnsureConnectionInTransactionAsync(CancellationToken token)
        {
            var connection = await EnsureConnectionAsync(token).ConfigureAwait(false);

            _transaction ??= await connection.BeginTransactionAsync(token).ConfigureAwait(false);

            return connection;
        }

        private sealed class TableContext : IAsyncDisposable
        {
            public string FullTableName { get; }

            public PreparedCommands Commands { get; }

            public TableContext(string fullTableName, PreparedCommands commands)
            {
                FullTableName = fullTableName;
                Commands = commands;
            }

            public ValueTask DisposeAsync() => Commands.DisposeAsync();
        }
    }
}
