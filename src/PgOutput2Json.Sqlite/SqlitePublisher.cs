using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;
using Microsoft.Data.Sqlite;

namespace PgOutput2Json.Sqlite
{
    public class SqlitePublisher : MessagePublisher, IRowPublisher
    {
        private readonly SqlitePublisherOptions _options;

        // the SQLite name of each PostgreSQL table (GetTableName), shared by the publishers of the factory
        internal TargetNames Names { get; init; }
        private readonly ILogger<SqlitePublisher>? _logger;

        private SqliteConnection? _connection;
        private DbTransaction? _transaction;

        private readonly Dictionary<string, TableContext> _tables = [];

        private ulong _lastWalEnd;
        private ulong _lastMessageNo;

        private readonly Utf8Buffer _utf8 = new();

        private string _tableName = "";

        // the values of the row and of the key of the change being applied
        private SqliteValue[] _row = [];
        private SqliteValue[] _key = [];

        // the JSON of an array value of a row
        private readonly StringBuilder _json = new();

        public SqlitePublisher(SqlitePublisherOptions options, ILogger<SqlitePublisher>? logger)
        {
            _options = options;
            Names = new TargetNames(options.GetTableName);
            _logger = logger;
        }

        public override async Task PublishAsync(JsonMessage msg, CancellationToken token)
        {
            var connection = await EnsureConnectionInTransactionAsync(token).ConfigureAwait(false);

            using var doc = JsonDocument.Parse(_utf8.Encode(msg.Json));

            _tableName = msg.TableName;

            await ApplyChangeAsync(connection, _tableName, msg.SchemaName, msg.RelationName, doc, token).ConfigureAwait(false);

            SetPosition(msg.IsTableSync, msg.TxFinalLsn, msg.MessageNo);
        }

        /// <summary>
        /// A row of the library, without its JSON: the values are PostgreSQL's text.
        /// </summary>
        public async Task PublishRowAsync(ChangeRow row, CancellationToken token)
        {
            var connection = await EnsureConnectionInTransactionAsync(token).ConfigureAwait(false);

            var table = await EnsureTableAsync(connection, row.Table, token).ConfigureAwait(false);

            var columns = table.Commands.Columns;

            var rowValues = row.NewValues != null ? ReadValues(row.NewValues, columns, keyOnly: false, ref _row) : [];
            var keyValues = row.OldValues != null ? ReadValues(row.OldValues, columns, keyOnly: true, ref _key) : null;

            var changeType = row.ChangeType switch
            {
                RowChangeType.Insert => "I",
                RowChangeType.Copy => "C",
                RowChangeType.Update => "U",
                _ => "D",
            };

            await ApplyChangeAsync(connection, table, changeType, keyValues, rowValues, token).ConfigureAwait(false);

            SetPosition(row.IsTableSync, row.TxFinalLsn, row.MessageNo);
        }

        private void SetPosition(bool isTableSync, ulong txFinalLsn, ulong messageNo)
        {
            // the messages of a table sync are behind the main stream - they must not move the saved position
            if (!isTableSync)
            {
                _lastWalEnd = txFinalLsn;
                _lastMessageNo = messageNo;
            }
        }

        /// <summary>
        /// The values of a row, or of the key columns of an old row (as the "k" of the JSON has them).
        /// </summary>
        private SqliteValue[] ReadValues(IReadOnlyList<RowValue> values, IReadOnlyList<ColumnInfo> columns, bool keyOnly, ref SqliteValue[] result)
        {
            var length = 0;

            for (var i = 0; i < columns.Count; i++)
            {
                if (!keyOnly || columns[i].IsKey) length++;
            }

            if (result.Length != length) result = new SqliteValue[length];

            var j = 0;

            for (var i = 0; i < columns.Count; i++)
            {
                if (keyOnly && !columns[i].IsKey) continue;

                result[j++] = SqliteValue.FromRow(values[i], columns[i].DataType, _json);
            }

            return result;
        }

        private async Task ApplyChangeAsync(SqliteConnection connection, string tableName, string schemaName, string relationName, JsonDocument doc, CancellationToken token)
        {
            doc.RootElement.TryGetProperty("c", out var changeTypeElement);

            var changeType = changeTypeElement.GetString();

            if (changeType == "M") return; // logical decoding message - carries no table and no row

            if (tableName.Length == 0) throw new Exception($"Missing table name for change type '{changeType}'");

            var table = await EnsureTableAsync(connection, tableName, schemaName, relationName, doc, token).ConfigureAwait(false);

            // read once: indexing a JSON array walks it from the start when it has arrays or objects in it
            var keyElement = doc.RootElement.TryGetProperty("k", out var key) && key.ValueKind == JsonValueKind.Array ? ReadValues(key, ref _key) : null;
            var rowElement = doc.RootElement.TryGetProperty("r", out var row) && row.ValueKind == JsonValueKind.Array ? ReadValues(row, ref _row) : [];

            await ApplyChangeAsync(connection, table, changeType, keyElement, rowElement, token).ConfigureAwait(false);
        }

        private static async Task ApplyChangeAsync(SqliteConnection connection, TableContext table, string? changeType, SqliteValue[]? keyElement, SqliteValue[] rowElement, CancellationToken token)
        {
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
                await connection.UpdateKeyAsync(table.Commands, table.Name, keyElement, rowElement, token).ConfigureAwait(false);

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
                    await _connection.SetWalEndAsync(_options.SubscriptionName, _lastWalEnd, _lastMessageNo, token).ConfigureAwait(false);
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

            return await cn.GetWalEndAsync(_options.SubscriptionName, token).ConfigureAwait(false);
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
        private static SqliteValue[] ReadValues(JsonElement array, ref SqliteValue[] values)
        {
            var length = array.GetArrayLength();

            if (values.Length != length) values = new SqliteValue[length];

            var i = 0;

            foreach (var value in array.EnumerateArray()) values[i++] = SqliteValue.FromJson(value);

            return values;
        }

        private static async Task UpdateRowAsync(SqliteConnection connection, TableContext table, SqliteValue[] rowElement, CancellationToken token)
        {
            var hasToast = HasUnchangedToast(rowElement);

            var updatedCount = hasToast
                ? await connection.FallbackUpdateAsync(table.Commands, table.Name, rowElement, token).ConfigureAwait(false)
                : await connection.UpdateAsync(table.Commands, rowElement, token).ConfigureAwait(false);

            if (updatedCount == 0)
            {
                // row does not exist - insert it (same behavior as update-or-insert)
                await connection.InsertAsync(table.Commands, rowElement, ignoreConflicts: false, token).ConfigureAwait(false);
            }
        }

        private static bool HasUnchangedToast(SqliteValue[] rowElement)
        {
            foreach (var value in rowElement)
            {
                if (value.Kind == SqliteValueKind.Unchanged) return true;
            }

            return false;
        }

        private async Task<TableContext> EnsureTableAsync(SqliteConnection connection, string tableName, string schemaName, string relationName, JsonDocument doc, CancellationToken token)
        {
            if (doc.RootElement.TryGetProperty("s", out var schemaElement))
            {
                // relation message - schema changed, rebuild the cached commands

                return await CreateTableContextAsync(connection, tableName, schemaName, relationName, ParseSchema(schemaElement), null, token).ConfigureAwait(false);
            }

            if (_tables.TryGetValue(tableName, out var cached)) return cached;

            // schema not cached - try loading it from the config table

            var storedColumns = await connection.GetTableSchemaAsync(tableName, token).ConfigureAwait(false);

            if (storedColumns == null) throw new Exception("Missing table schema: " + tableName);

            var sqliteName = Names.Get(tableName, schemaName, relationName);

            var restored = new TableContext(tableName, sqliteName, connection.CreatePreparedCommands(sqliteName, storedColumns));

            _tables[tableName] = restored;

            return restored;
        }

        /// <summary>
        /// The table of a row: the same <see cref="RowTable"/> until the columns may have changed.
        /// </summary>
        private async Task<TableContext> EnsureTableAsync(SqliteConnection connection, RowTable rowTable, CancellationToken token)
        {
            if (_tables.TryGetValue(rowTable.Name, out var cached) && cached.Source == rowTable) return cached;

            var columns = new List<ColumnInfo>(rowTable.Columns.Count);

            foreach (var column in rowTable.Columns)
            {
                columns.Add(new ColumnInfo { Name = column.Name, DataType = column.TypeId, TypeModifier = column.TypeModifier, IsKey = column.IsKey });
            }

            return await CreateTableContextAsync(connection, rowTable.Name, rowTable.SchemaName, rowTable.RelationName, columns, rowTable, token).ConfigureAwait(false);
        }

        private async Task<TableContext> CreateTableContextAsync(SqliteConnection connection, string tableName, string schemaName, string relationName,
                                                                 List<ColumnInfo> columns, RowTable? source, CancellationToken token)
        {
            var sqliteName = Names.Get(tableName, schemaName, relationName);

            var table = new TableContext(tableName, sqliteName, connection.CreatePreparedCommands(sqliteName, columns)) { Source = source };

            if (_tables.Remove(tableName, out var existing))
            {
                await existing.DisposeAsync().ConfigureAwait(false);
            }

            _tables[tableName] = table;

            await connection.CreateOrAlterTableAsync(sqliteName, columns, token).ConfigureAwait(false);

            await connection.SetSchemaAsync(tableName, columns, token).ConfigureAwait(false);

            return table;
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

            /// <summary>The name of the SQLite table (GetTableName).</summary>
            public string Name { get; }

            public PreparedCommands Commands { get; }

            /// <summary>The <see cref="RowTable"/> of the rows, for an <see cref="IRowPublisher"/> row.</summary>
            public RowTable? Source { get; init; }

            public TableContext(string fullTableName, string name, PreparedCommands commands)
            {
                FullTableName = fullTableName;
                Name = name;
                Commands = commands;
            }

            public ValueTask DisposeAsync() => Commands.DisposeAsync();
        }
    }
}
