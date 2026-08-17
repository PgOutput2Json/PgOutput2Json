using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Data.Sqlite;

namespace PgOutput2Json.Sqlite
{
    internal static class SqliteConnectionExtensions
    {
        private const string ToastValue = "__TOAST__";

        public static async Task<(ulong, ulong)> GetWalEndAsync(this SqliteConnection cn, CancellationToken token)
        {
            var walEndValue = await GetConfigAsync(cn, ConfigKey.WalEnd, token).ConfigureAwait(false);
            var messageNoValue = await GetConfigAsync(cn, ConfigKey.MessageNo, token).ConfigureAwait(false);

            return (walEndValue != null ? ulong.Parse(walEndValue, CultureInfo.InvariantCulture) : 0,
                    messageNoValue != null ? ulong.Parse(messageNoValue, CultureInfo.InvariantCulture) : 0);
        }

        public static async Task SetWalEndAsync(this SqliteConnection cn, ulong walEnd, ulong messageNo, CancellationToken token)
        {
            await SaveConfigAsync(cn, ConfigKey.WalEnd, walEnd.ToString(CultureInfo.InvariantCulture), token).ConfigureAwait(false);
            await SaveConfigAsync(cn, ConfigKey.MessageNo, messageNo.ToString(CultureInfo.InvariantCulture), token).ConfigureAwait(false);
        }

        public static async Task SetSchemaAsync(this SqliteConnection cn, string tableName, IReadOnlyList<ColumnInfo> cols, CancellationToken token)
        {
#pragma warning disable VSTHRD103 // Call async methods when in an async method
            var json = JsonSerializer.Serialize(cols, JsonContext.Default.ListColumnInfo);
#pragma warning restore VSTHRD103 // Call async methods when in an async method

            await SaveConfigAsync(cn, $"{ConfigKey.Schema}_{tableName}", json, token).ConfigureAwait(false);
        }

        public static async Task<List<ColumnInfo>?> GetTableSchemaAsync(this SqliteConnection cn, string tableName, CancellationToken token)
        {
            var cfgValue = await GetConfigAsync(cn, $"{ConfigKey.Schema}_{tableName}", token).ConfigureAwait(false);

            if (cfgValue == null) return null;

#pragma warning disable VSTHRD103 // Call async methods when in an async method
            return JsonSerializer.Deserialize(cfgValue, JsonContext.Default.ListColumnInfo);
#pragma warning restore VSTHRD103 // Call async methods when in an async method
        }

        public static async Task SaveConfigAsync(this SqliteConnection cn, string key, string value, CancellationToken token)
        {
            using var cmd = cn.CreateCommand();

            cmd.Parameters.AddWithValue("cfg_key", key);
            cmd.Parameters.AddWithValue("cfg_value", value);

            cmd.CommandText = "UPDATE __pg2j_config SET cfg_value = @cfg_value WHERE cfg_key = @cfg_key";

            var result = await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);

            if (result == 0)
            {
                cmd.CommandText = "INSERT INTO __pg2j_config (cfg_key, cfg_value) VALUES (@cfg_key, @cfg_value)";

                await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
            }
        }

        public static async Task<string?> GetConfigAsync(this SqliteConnection cn, string key, CancellationToken token)
        {
            using var cmd = cn.CreateCommand();

            cmd.Parameters.AddWithValue("cfg_key", key);

            cmd.CommandText = "SELECT cfg_value FROM __pg2j_config WHERE cfg_key = @cfg_key";

            var result = await cmd.ExecuteScalarAsync(token).ConfigureAwait(false);

            return result?.ToString();
        }

        public static async Task UseWalAsync(this SqliteConnection cn, CancellationToken token)
        {
            using var cmd = cn.CreateCommand();

            cmd.CommandText = @"PRAGMA journal_mode=WAL;";

            await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }

        public static async Task WalCheckpointAsync(this SqliteConnection cn, WalCheckpointType checkpointType, int tryCount, CancellationToken token)
        {
            if (checkpointType == WalCheckpointType.Automatic) return; // nothing to do

            using var cmd = cn.CreateCommand();

            var typeStr = checkpointType == WalCheckpointType.Passive ? "PASSIVE"
                : checkpointType == WalCheckpointType.Truncate ? "TRUNCATE"
                : checkpointType == WalCheckpointType.Restart ? "RESTART"
                : "FULL";

            cmd.CommandText = @$"PRAGMA wal_checkpoint({typeStr});";

            var success = false;

            while (--tryCount >= 0)
            {
                using var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);

                if (await reader.ReadAsync(token).ConfigureAwait(false))
                {
                    if (reader.GetInt32(0) == 0) // not busy
                    {
                        success = true;
                        break;
                    }
                }
            }

            if (!success) throw new Exception("Could not perform checkpoint - SQLite busy");
        }


        public static async Task CreateConfigTableAsync(this SqliteConnection cn, CancellationToken token)
        {
            using var cmd = cn.CreateCommand();

            cmd.CommandText = @"
CREATE TABLE IF NOT EXISTS __pg2j_config (
    cfg_key TEXT NOT NULL,
    cfg_value TEXT,
    CONSTRAINT __pg2j_config_pk PRIMARY KEY (cfg_key)
)";

            await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }

        public static async Task CreateOrAlterTableAsync(this SqliteConnection cn, string fullTableName, IReadOnlyList<ColumnInfo> columns, CancellationToken token)
        {
            var tableName = GetTableName(fullTableName);

            var exists = false;

            using (var cmd = cn.CreateCommand())
            {
                cmd.CommandText = $"SELECT 1 FROM sqlite_master WHERE type='table' AND name='{tableName}'";

                exists = await cmd.ExecuteScalarAsync(token).ConfigureAwait(false) != null;
            }

            if (exists)
            {
                await AlterTableAsync(cn, tableName, columns, token).ConfigureAwait(false);
            }
            else
            {
                await CreateTableAsync(cn, tableName, columns, token).ConfigureAwait(false);
            }
        }

        private static async Task AlterTableAsync(SqliteConnection cn, string tableName, IReadOnlyList<ColumnInfo> columns, CancellationToken token)
        {
            var existingCols = new List<string>();

            using (var cmd = cn.CreateCommand())
            {
                cmd.CommandText = $"PRAGMA table_info(\"{tableName}\")";

                using var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);

                while (await reader.ReadAsync(token).ConfigureAwait(false))
                {
                    // The column name is in the second column (index 1)
                    existingCols.Add(reader.GetString(1));
                }
            }

            using var alterCmd = cn.CreateCommand();

            foreach (var col in columns.Where(c => !existingCols.Contains(c.Name)))
            {
                alterCmd.CommandText = $"ALTER TABLE \"{tableName}\" ADD \"{col.Name}\" {col.GetSqliteType()}";

                await alterCmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
            }
        }

        private static async Task CreateTableAsync(SqliteConnection cn, string tableName, IReadOnlyList<ColumnInfo> columns, CancellationToken token)
        {
            var sqlBuilder = new StringBuilder(256);
            var keyBuilder = new StringBuilder(256);

            sqlBuilder.Append($"CREATE TABLE IF NOT EXISTS \"{tableName}\" (");

            var i = 0;
            foreach (var colInfo in columns)
            {
                if (i > 0) sqlBuilder.Append(", ");
                sqlBuilder.Append($"\"{colInfo.Name}\" {colInfo.GetSqliteType()}");

                if (colInfo.IsKey)
                {
                    if (keyBuilder.Length > 0) keyBuilder.Append(", ");
                    keyBuilder.Append($"\"{colInfo.Name}\"");
                }
                i++;
            }

            if (keyBuilder.Length > 0)
            {
                sqlBuilder.Append($", CONSTRAINT \"pk_{tableName}\" PRIMARY KEY (");
                sqlBuilder.Append(keyBuilder);
                sqlBuilder.Append(')');
            }

            sqlBuilder.Append(')');

            using var cmd = cn.CreateCommand();

            cmd.CommandText = sqlBuilder.ToString();

            await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }

        private static string GetTableName(string fullTableName)
        {
            var nameParts = fullTableName.Split('.');
            return nameParts.Length > 1 ? nameParts[1] : nameParts[0];
        }

        /// <summary>
        /// Creates prepared parameterized commands for inserting/updating/deleting rows of the table.
        /// The commands are cached and reused until a relation message (schema change) is received.
        /// They are created without a transaction - SQLite applies the connection's active transaction at execution time.
        /// </summary>
        public static PreparedCommands CreatePreparedCommands(this SqliteConnection cn, string fullTableName, IReadOnlyList<ColumnInfo> columns)
        {
            var tableName = GetTableName(fullTableName);

            var keyCount = 0;
            var nonKeyCount = 0;

            foreach (var col in columns)
            {
                if (col.IsKey) keyCount++;
                else nonKeyCount++;
            }

            var insertBuilder = new StringBuilder(256);

            insertBuilder.Append($"INSERT INTO \"{tableName}\" (");

            var i = 0;
            foreach (var col in columns)
            {
                if (i > 0) insertBuilder.Append(", ");
                insertBuilder.Append($"\"{col.Name}\"");
                i++;
            }

            insertBuilder.Append(") VALUES (");

            for (i = 0; i < columns.Count; i++)
            {
                if (i > 0) insertBuilder.Append(", ");
                insertBuilder.Append($"@p{i}");
            }

            insertBuilder.Append(')');

            var insertSql = insertBuilder.ToString();

            SqliteCommand? updateCommand = null;

            if (keyCount > 0 && nonKeyCount > 0)
            {
                // SET parameters come first (@p0..@p{nonKeyCount-1}), then WHERE parameters

                var updateBuilder = new StringBuilder($"UPDATE \"{tableName}\" SET ");

                var j = 0;
                foreach (var col in columns)
                {
                    if (col.IsKey) continue;

                    if (j > 0) updateBuilder.Append(", ");
                    updateBuilder.Append($"\"{col.Name}\" = @p{j}");
                    j++;
                }

                updateBuilder.Append(" WHERE ");
                updateBuilder.Append(BuildKeyWhereClause(columns, nonKeyCount));

                updateCommand = CreateCommand(cn, updateBuilder.ToString(), columns.Count);
            }

            var deleteCommand = keyCount > 0
                ? CreateCommand(cn, $"DELETE FROM \"{tableName}\" WHERE {BuildKeyWhereClause(columns, 0)}", keyCount)
                : null;

            return new PreparedCommands(
                CreateCommand(cn, insertSql, columns.Count),
                CreateCommand(cn, insertSql + " ON CONFLICT DO NOTHING", columns.Count),
                updateCommand,
                deleteCommand,
                columns);
        }

        private static SqliteCommand CreateCommand(SqliteConnection cn, string sql, int paramCount)
        {
            var cmd = cn.CreateCommand();

            cmd.CommandText = sql;

            for (var i = 0; i < paramCount; i++)
            {
                cmd.Parameters.AddWithValue($"@p{i}", DBNull.Value);
            }

            return cmd;
        }

        /// <summary>
        /// Builds a NULL-safe equality clause over the key columns ("col" = @pX OR (@pX IS NULL AND "col" IS NULL)).
        /// </summary>
        private static string BuildKeyWhereClause(IReadOnlyList<ColumnInfo> columns, int paramOffset)
        {
            var whereBuilder = new StringBuilder();

            var keyIndex = 0;
            foreach (var col in columns)
            {
                if (!col.IsKey) continue;

                if (keyIndex > 0) whereBuilder.Append(" AND ");

                var paramName = $"@p{paramOffset + keyIndex}";

                whereBuilder.Append($"(\"{col.Name}\" = {paramName} OR ({paramName} IS NULL AND \"{col.Name}\" IS NULL))");

                keyIndex++;
            }

            return whereBuilder.ToString();
        }

        public static async Task InsertAsync(this SqliteConnection cn, PreparedCommands commands, JsonElement rowElement, bool ignoreConflicts, CancellationToken token)
        {
            var cmd = ignoreConflicts ? commands.InsertIgnoreCommand : commands.InsertCommand;

            for (var i = 0; i < commands.Columns.Count; i++)
            {
                BindValue(cmd.Parameters[i], rowElement[i], commands.Columns[i]);
            }

            await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }

        /// <summary>
        /// Updates the row using the prepared update command.
        /// Returns the number of updated rows.
        /// </summary>
        public static async Task<int> UpdateAsync(this SqliteConnection cn, PreparedCommands commands, JsonElement rowElement, CancellationToken token)
        {
            var updateCommand = commands.UpdateCommand;
            if (updateCommand == null) return 0;

            var columns = commands.Columns;

            // SET parameters come first (non-key columns), then WHERE parameters (key columns)

            var paramIndex = 0;

            for (var i = 0; i < columns.Count; i++)
            {
                if (columns[i].IsKey) continue;

                BindValue(updateCommand.Parameters[paramIndex], rowElement[i], columns[i]);
                paramIndex++;
            }

            for (var i = 0; i < columns.Count; i++)
            {
                if (!columns[i].IsKey) continue;

                BindValue(updateCommand.Parameters[paramIndex], rowElement[i], columns[i]);
                paramIndex++;
            }

            return await updateCommand.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }

        /// <summary>
        /// Updates the row, skipping "__TOAST__" values (unchanged toasted columns).
        /// Used when some row values are toasted (unchanged and not sent by PostgreSQL).
        /// Returns the number of updated rows.
        /// </summary>
        public static async Task<int> FallbackUpdateAsync(this SqliteConnection cn, PreparedCommands commands, string fullTableName, JsonElement rowElement, CancellationToken token)
        {
            var tableName = GetTableName(fullTableName);
            var columns = commands.Columns;

            var setCols = new List<int>();

            for (var i = 0; i < columns.Count; i++)
            {
                if (columns[i].IsKey) continue;

                if (rowElement[i].ValueKind == JsonValueKind.String && rowElement[i].GetString() == ToastValue)
                {
                    continue; // skip unchanged toasted columns
                }

                setCols.Add(i);
            }

            if (setCols.Count == 0) return 1; // nothing to update

            var sqlBuilder = new StringBuilder($"UPDATE \"{tableName}\" SET ");

            var j = 0;
            foreach (var colIndex in setCols)
            {
                if (j > 0) sqlBuilder.Append(", ");
                sqlBuilder.Append($"\"{columns[colIndex].Name}\" = @p{j}");
                j++;
            }

            sqlBuilder.Append(" WHERE ");
            sqlBuilder.Append(BuildKeyWhereClause(columns, setCols.Count));

            using var cmd = cn.CreateCommand();

            cmd.CommandText = sqlBuilder.ToString();

            var paramIndex = 0;

            foreach (var colIndex in setCols)
            {
                cmd.Parameters.AddWithValue($"@p{paramIndex}", DBNull.Value);
                BindValue(cmd.Parameters[paramIndex], rowElement[colIndex], columns[colIndex]);
                paramIndex++;
            }

            for (var i = 0; i < columns.Count; i++)
            {
                if (!columns[i].IsKey) continue;

                cmd.Parameters.AddWithValue($"@p{paramIndex}", DBNull.Value);
                BindValue(cmd.Parameters[paramIndex], rowElement[i], columns[i]);
                paramIndex++;
            }

            return await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }

        public static async Task DeleteAsync(this SqliteConnection cn, PreparedCommands commands, JsonElement keyElement, CancellationToken token)
        {
            var deleteCommand = commands.DeleteCommand;
            if (deleteCommand == null || keyElement.ValueKind != JsonValueKind.Array) return;

            var columns = commands.Columns;

            var keyIndex = 0;

            for (var i = 0; i < columns.Count; i++)
            {
                if (!columns[i].IsKey) continue;

                BindValue(deleteCommand.Parameters[keyIndex], keyElement[keyIndex], columns[i]);

                keyIndex++;
            }

            await deleteCommand.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }

        private static void BindValue(SqliteParameter param, JsonElement valElement, ColumnInfo column)
        {
            switch (valElement.ValueKind)
            {
                case JsonValueKind.Undefined:
                case JsonValueKind.Null:
                    param.Value = DBNull.Value;
                    break;
                case JsonValueKind.Number:
                    if (valElement.TryGetInt64(out var intValue))
                    {
                        param.Value = intValue;
                    }
                    else if (valElement.TryGetDecimal(out var decimalValue))
                    {
                        param.Value = decimalValue;
                    }
                    else
                    {
                        param.Value = valElement.GetRawText();
                    }
                    break;
                case JsonValueKind.True:
                    param.Value = 1L;
                    break;
                case JsonValueKind.False:
                    param.Value = 0L;
                    break;
                case JsonValueKind.String:
                    if (column.IsDateTime())
                    {
                        var dateTime = DateTimeOffset.Parse(valElement.GetString() ?? "1970-01-01 00:00:00", CultureInfo.InvariantCulture);
                        param.Value = dateTime.ToString("O");
                    }
                    else if (column.IsBlob())
                    {
                        param.Value = ParseHexBlob(valElement.GetString());
                    }
                    else
                    {
                        param.Value = valElement.GetString() ?? "";
                    }
                    break;
                default:
                    param.Value = valElement.GetRawText();
                    break;
            }
        }

        private static byte[] ParseHexBlob(string? hexValue)
        {
            if (hexValue == null) return [];

            var bytes = new byte[hexValue.Length / 2];

            for (var i = 0; i < bytes.Length; i++)
            {
                bytes[i] = Convert.ToByte(hexValue.Substring(i * 2, 2), 16);
            }

            return bytes;
        }
    }

    /// <summary>
    /// Prepared commands for a table, created from its schema.
    /// Invalidated and rebuilt when a relation message (schema change) is received.
    /// </summary>
    internal sealed class PreparedCommands : IAsyncDisposable
    {
        public IReadOnlyList<ColumnInfo> Columns { get; }

        public SqliteCommand InsertCommand { get; }

        public SqliteCommand InsertIgnoreCommand { get; }

        /// <summary>Null when the table has no key columns or no non-key columns.</summary>
        public SqliteCommand? UpdateCommand { get; }

        /// <summary>Null when the table has no key columns.</summary>
        public SqliteCommand? DeleteCommand { get; }

        public PreparedCommands(SqliteCommand insertCommand, SqliteCommand insertIgnoreCommand, SqliteCommand? updateCommand, SqliteCommand? deleteCommand, IReadOnlyList<ColumnInfo> columns)
        {
            InsertCommand = insertCommand;
            InsertIgnoreCommand = insertIgnoreCommand;
            UpdateCommand = updateCommand;
            DeleteCommand = deleteCommand;
            Columns = columns;
        }

        public async ValueTask DisposeAsync()
        {
            await InsertCommand.DisposeAsync().ConfigureAwait(false);
            await InsertIgnoreCommand.DisposeAsync().ConfigureAwait(false);

            if (UpdateCommand != null)
            {
                await UpdateCommand.DisposeAsync().ConfigureAwait(false);
            }

            if (DeleteCommand != null)
            {
                await DeleteCommand.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    public struct ColumnInfo
    {
        public string Name { get; set; }
        public bool IsKey { get; set; }
        public uint DataType { get; set; }
        public int TypeModifier { get; set; }

        public readonly string GetSqliteType()
        {
            if (!Enum.IsDefined(typeof(PgOid), DataType)) return "TEXT";

            var pgOid = (PgOid)DataType;

            return pgOid switch
            {
                PgOid.BOOLOID => "INTEGER",  // BOOLEAN maps to INTEGER in SQLite
                PgOid.BYTEAOID => "BLOB",    // BYTEA maps to BLOB in SQLite
                PgOid.INT8OID => "INTEGER",  // BIGINT maps to INTEGER
                PgOid.INT2OID => "INTEGER",  // SMALLINT maps to INTEGER
                PgOid.INT4OID => "INTEGER",  // INTEGER maps to INTEGER
                PgOid.OIDOID => "INTEGER",   // OID maps to INTEGER
                PgOid.FLOAT4OID => "REAL",   // FLOAT4 maps to REAL
                PgOid.FLOAT8OID => "REAL",   // FLOAT8 maps to REAL
                PgOid.NUMERICOID => "NUMERIC" + GetPrecisionAndScale(TypeModifier),
                PgOid.TIMESTAMPOID => "DATETIME",
                PgOid.TIMESTAMPTZOID => "DATETIME",
                _ => "TEXT",// Default fallback for unknown types
            };
        }

        public readonly bool IsDateTime()
        {
            return ((PgOid)DataType).IsTimestamp();
        }

        public readonly bool IsBlob()
        {
            return ((PgOid)DataType) == PgOid.BYTEAOID;
        }


        private static string GetPrecisionAndScale(int typeModifier)
        {
            // Precision is stored in the upper 16 bits
            int precision = (typeModifier >> 16) & 0xFFFF;

            // Scale is stored in the lower 16 bits
            int scale = typeModifier & 0xFFFF;

            return $"({precision}, {scale})";
        }
    }
}
