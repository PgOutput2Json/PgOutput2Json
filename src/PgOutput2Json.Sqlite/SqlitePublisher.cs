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

        private readonly Utf8Buffer _utf8 = new();

        private string _tableName = "";

        // the values of the row and of the key of the change being applied
        private JsonElement[] _row = [];
        private JsonElement[] _key = [];

        public SqlitePublisher(SqlitePublisherOptions options, ILogger<SqlitePublisher>? logger)
        {
            _options = options;
            _logger = logger;
        }

        public override async Task PublishAsync(JsonMessage msg, CancellationToken token)
        {
            var connection = await EnsureConnectionInTransactionAsync(token).ConfigureAwait(false);

            using var doc = JsonDocument.Parse(_utf8.Encode(msg.Json));

            // the rows of a batch are mostly of one table: its name is not made again for each of them
            if (!msg.TableName.Equals(_tableName.AsSpan())) _tableName = msg.TableName.ToString();

            await ApplyChangeAsync(connection, _tableName, doc, token).ConfigureAwait(false);

            // the messages of a table sync are behind the main stream - they must not move the saved position
            if (!msg.IsTableSync)
            {
                _lastWalEnd = msg.TxFinalLsn;
                _lastMessageNo = msg.MessageNo;
            }
        }

        private async Task ApplyChangeAsync(SqliteConnection connection, string tableName, JsonDocument doc, CancellationToken token)
        {
            doc.RootElement.TryGetProperty("c", out var changeTypeElement);

            var changeType = changeTypeElement.GetString();

            if (changeType == "M") return; // logical decoding message - carries no table and no row

            if (tableName.Length == 0) throw new Exception($"Missing table name for change type '{changeType}'");

            var table = await EnsureTableAsync(connection, tableName, doc, token).ConfigureAwait(false);

            // read once: indexing a JSON array walks it from the start when it has arrays or objects in it
            var keyElement = doc.RootElement.TryGetProperty("k", out var key) && key.ValueKind == JsonValueKind.Array ? ReadValues(key, ref _key) : null;
            var rowElement = doc.RootElement.TryGetProperty("r", out var row) && row.ValueKind == JsonValueKind.Array ? ReadValues(row, ref _row) : [];

            if (changeType == "I")
            {
                await connection.InsertAsync(table.Commands, rowElement, ignoreConflicts: true, token).ConfigureAwait(false);
            }
            else if (changeType == "C")
            {
                // a copied row replaces what an earlier, failed copy may have left
                if (await connection.InsertAsync(table.Commands, rowElement, ignoreConflicts: true, token).ConfigureAwait(false) == 0)
                {
                    await connection.UpdateAsync(table.Commands, rowElement, token).ConfigureAwait(false);
                }
            }
            else if (changeType == "U")
            {
                // an update of the key comes with the old key: the row is moved first, or the old one would stay
                await connection.UpdateKeyAsync(table.Commands, table.FullTableName, keyElement, rowElement, token).ConfigureAwait(false);

                await UpdateRowAsync(connection, table, rowElement, token).ConfigureAwait(false);
            }
            else if (changeType == "D")
            {
                await connection.DeleteAsync(table.Commands, keyElement, token).ConfigureAwait(false);
            }
        }

        public override async Task ConfirmAsync(CancellationToken token)
        {
            if (_transaction == null) return;

            try
            {
                // nothing to save if the batch had only table sync messages
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

                // prepared commands keep a reference to the transaction they are bound to -
                // clear it so they never hold a completed transaction
                foreach (var table in _tables.Values)
                {
                    table.Commands.SetTransaction(null);
                }
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

        private static async Task UpdateRowAsync(SqliteConnection connection, TableContext table, JsonElement[] rowElement, CancellationToken token)
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

        private static bool HasUnchangedToast(JsonElement[] rowElement)
        {
            foreach (var value in rowElement)
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

            // a connection of its own: a pooled one keeps its settings, so it could bring those of the application's
            // connections (PRAGMA query_only), and take ours to them
            var connectionString = new SqliteConnectionStringBuilder(_options.ConnectionStringBuilder.ConnectionString) { Pooling = false };

            _connection = new SqliteConnection(connectionString.ConnectionString);

            await _connection.OpenAsync(token).ConfigureAwait(false);

            // the rows come in the order of the table syncs and the batches, not in the order of the foreign keys,
            // so they are not checked, as with session_replication_role = replica in PostgreSQL's own replication;
            // the setting is per connection, and is ignored inside a transaction
            await _connection.DisableForeignKeysAsync(token).ConfigureAwait(false);

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

            if (_transaction == null)
            {
                var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);

                _transaction = transaction;

                // cached commands are bound to the transaction they were created in -
                // they must be re-bound or execution fails with TransactionConnectionMismatch
                foreach (var table in _tables.Values)
                {
                    table.Commands.SetTransaction((SqliteTransaction)transaction);
                }
            }

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
