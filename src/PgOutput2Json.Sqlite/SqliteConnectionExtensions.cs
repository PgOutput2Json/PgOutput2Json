using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;

namespace PgOutput2Json.Sqlite
{
    internal static class SqliteConnectionExtensions
    {
        public static async Task<ulong> GetWalEndAsync(this SqliteConnection cn, CancellationToken token)
        {
            var cfgValue = await GetConfigAsync(cn, ConfigKey.WalEnd, token).ConfigureAwait(false);
            
            return cfgValue != null ? ulong.Parse(cfgValue, CultureInfo.InvariantCulture) : 0;
        }

        public static async Task SetWalEndAsync(this SqliteConnection cn, ulong walEnd, CancellationToken token)
        {
            await SaveConfigAsync(cn, ConfigKey.WalEnd, walEnd.ToString(CultureInfo.InvariantCulture), token).ConfigureAwait(false);
        }

        public static async Task SetSchemaAsync(this SqliteConnection cn, string tableName, IReadOnlyList<ColumnInfo> cols, CancellationToken token)
        {
            await SaveConfigAsync(cn, $"{ConfigKey.Schema}_{tableName}", JsonSerializer.Serialize(cols), token).ConfigureAwait(false);
        }

        public static async Task<List<ColumnInfo>?> GetSchemaAsync(this SqliteConnection cn, string tableName, CancellationToken token)
        {
            var cfgValue = await GetConfigAsync(cn, $"{ConfigKey.Schema}_{tableName}", token).ConfigureAwait(false);

            if (cfgValue == null) return null;

            return JsonSerializer.Deserialize<List<ColumnInfo>>(cfgValue);
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
            using var cmd = cn.CreateCommand();

            var typeStr = checkpointType == WalCheckpointType.Truncate ? "TRUNCATE"
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
            string tableName = GetTableName(fullTableName);

            using var cmd = cn.CreateCommand();

            cmd.CommandText = $"SELECT 1 FROM sqlite_master WHERE type='table' AND name='{tableName}'";

            if (await cmd.ExecuteScalarAsync(token).ConfigureAwait(false) != null)
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
            var cmd = cn.CreateCommand();

            cmd.CommandText = $"PRAGMA table_info(\"{tableName}\")";

            var existingCols = new List<string>();

            using (var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false))
            {
                while (await reader.ReadAsync(token).ConfigureAwait(false))
                {
                    // The column name is in the second column (index 1)
                    existingCols.Add(reader.GetString(1)); 
                }
            }

            foreach (var col in columns.Where(c => !existingCols.Contains(c.Name)))
            {
                cmd.CommandText = $"ALTER TABLE \"{tableName}\" ADD \"{col.Name}\" {col.GetSqliteType()}";

                await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
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

        public static async Task UpdateOrInsertAsync(this SqliteConnection cn,
                                                     ulong walEnd,
                                                     string fullTableName,
                                                     IReadOnlyList<ColumnInfo> columns,
                                                     JsonElement changeTypeElement,
                                                     JsonElement keyElement,
                                                     JsonElement rowElement,
                                                     ILogger? logger,
                                                     CancellationToken token)
        {
            var changeType = changeTypeElement.GetString();

            if (changeType == "I")
            {
                await cn.InsertAsync(fullTableName, columns, rowElement, true, token).ConfigureAwait(false);
            }
            else if (changeType == "U")
            {
                var count = await cn.UpdateAsync(fullTableName, columns, keyElement, rowElement, token).ConfigureAwait(false);
                if (count == 0)
                {
                    await cn.InsertAsync(fullTableName, columns, rowElement, false, token).ConfigureAwait(false);
                }
            }
            else if (changeType == "D")
            {
                await cn.DeleteAsync(fullTableName, columns, keyElement, token).ConfigureAwait(false);
            }

            await cn.SetWalEndAsync(walEnd, token).ConfigureAwait(false);
        }

        public static async Task InsertAsync(this SqliteConnection cn, string fullTableName, IReadOnlyList<ColumnInfo> columns, JsonElement rowElement, bool ignoreConflicts, CancellationToken token)
        {
            var tableName = GetTableName(fullTableName);

            var blobs = new List<(ColumnInfo Col, string Val)>();

            var sqlBuilder = new StringBuilder($"INSERT INTO \"{tableName}\" (");

            int i;

            i = 0;
            foreach (var column in columns)
            {
                if (i > 0) sqlBuilder.Append(", ");
                sqlBuilder.Append($"\"{column.Name}\"");

                i++;
            }

            sqlBuilder.Append(") VALUES (");

            i = 0;
            foreach (var valElement in rowElement.EnumerateArray())
            {
                if (i > 0) sqlBuilder.Append(", ");

                WriteColumnValue(sqlBuilder, valElement, columns[i], blobs);

                i++;
            }

            sqlBuilder.Append(')');

            if (ignoreConflicts)
            {
                sqlBuilder.Append(" ON CONFLICT DO NOTHING");
            }

            using var cmd = cn.CreateCommand();

            cmd.CommandText = sqlBuilder.ToString();

            var affectedCount = await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);

            if (affectedCount > 0 && blobs.Count > 0)
            {
                cmd.CommandText = "SELECT last_insert_rowid()";

                var rowId = Convert.ToInt64(await cmd.ExecuteScalarAsync(token).ConfigureAwait(false) ?? 0L);

                // store blobs;
                StoreBlobs(cn, tableName, rowId, blobs);
            }
        }

        public static async Task<int> UpdateAsync(this SqliteConnection cn, string fullTableName, IReadOnlyList<ColumnInfo> columns, JsonElement keyElement, JsonElement rowElement, CancellationToken token)
        {
            var tableName = GetTableName(fullTableName);

            var blobs = new List<(ColumnInfo Col, string Val)>();

            var setFieldsBuilder = new StringBuilder();
            var whereBuilder = new StringBuilder();

            int i;

            if (keyElement.ValueKind != JsonValueKind.Undefined)
            {
                i = 0;
                foreach (var column in columns)
                {
                    if (!column.IsKey) continue;

                    if (whereBuilder.Length > 0) whereBuilder.Append(" AND ");

                    WriteColumnValueAssignment(whereBuilder, keyElement[i], column, null, true);

                    i++;
                }
            }

            var hasWhere = whereBuilder.Length > 0; 

            i = 0;
            foreach (var column in columns)
            {
                if (column.IsKey && !hasWhere)
                {
                    if (whereBuilder.Length > 0) whereBuilder.Append(" AND ");

                    WriteColumnValueAssignment(whereBuilder, rowElement[i], column, null);

                    i++;
                    continue;
                }

                if (rowElement[i].ValueKind == JsonValueKind.String && rowElement[i].GetString() == "__TOAST__")
                {
                    i++;
                    continue; // skip unchanged toasted columns
                }

                if (setFieldsBuilder.Length > 0) setFieldsBuilder.Append(", ");

                WriteColumnValueAssignment(setFieldsBuilder, rowElement[i], column, blobs);

                i++;
            }

            var sqlBuilder = new StringBuilder($"UPDATE \"{tableName}\" SET ");
            sqlBuilder.Append(setFieldsBuilder);
            sqlBuilder.Append(" WHERE ");
            sqlBuilder.Append(whereBuilder);

            using var cmd = cn.CreateCommand();

            cmd.CommandText = sqlBuilder.ToString();

            var affectedCount = await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);

            if (affectedCount > 0 && blobs.Count > 0)
            {
                sqlBuilder.Clear();
                sqlBuilder.Append($"SELECT ROWID FROM \"{tableName}\" WHERE ");
                sqlBuilder.Append(whereBuilder);

                cmd.CommandText = sqlBuilder.ToString();

                var rowId = Convert.ToInt64(await cmd.ExecuteScalarAsync(token).ConfigureAwait(false) ?? 0);

                StoreBlobs(cn, tableName, rowId, blobs);
            }

            return affectedCount;
        }

        public static async Task DeleteAsync(this SqliteConnection cn, string fullTableName, IReadOnlyList<ColumnInfo> columns, JsonElement keyElement, CancellationToken token)
        {
            var tableName = GetTableName(fullTableName);

            var sqlBuilder = new StringBuilder($"DELETE FROM \"{tableName}\" WHERE ");

            var i = 0;

            foreach (var column in columns)
            {
                if (!column.IsKey) continue;

                if (i > 0) sqlBuilder.Append(" AND ");

                WriteColumnValueAssignment(sqlBuilder, keyElement[i], column, null, true);

                i++;
            }

            using var cmd = cn.CreateCommand();

            cmd.CommandText = sqlBuilder.ToString();

            await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }

        private static void WriteColumnValueAssignment(StringBuilder keysBuilder, JsonElement rowElement, ColumnInfo column, List<(ColumnInfo, string)>? blobs, bool isWhereStatement = false)
        {
            if (isWhereStatement && rowElement.ValueKind == JsonValueKind.Null)
            {
                keysBuilder.Append($"\"{column.Name}\" IS NULL");
            }
            else
            {
                keysBuilder.Append($"\"{column.Name}\"=");
                WriteColumnValue(keysBuilder, rowElement, column, blobs);
            }
        }

        private static void WriteColumnValue(StringBuilder sqlBuilder, JsonElement valElement, ColumnInfo column, List<(ColumnInfo, string)>? blobs)
        {
            switch (valElement.ValueKind)
            {
                case JsonValueKind.Undefined:
                case JsonValueKind.Null:
                    sqlBuilder.Append("NULL");
                    break;
                case JsonValueKind.Number:
                    if (valElement.TryGetInt64(out var intValue))
                    {
                        sqlBuilder.Append(intValue.ToString(CultureInfo.InvariantCulture));
                    }
                    else
                    {
                        valElement.TryGetDecimal(out var decimalValue);
                        sqlBuilder.Append(decimalValue.ToString(CultureInfo.InvariantCulture));
                    }
                    break;
                case JsonValueKind.True:
                    sqlBuilder.Append('1');
                    break;
                case JsonValueKind.False:
                    sqlBuilder.Append('0');
                    break;
                case JsonValueKind.String:
                    if (column.IsDateTime())
                    {
                        var dateTime = DateTimeOffset.Parse(valElement.GetString() ?? "1970-01-01 00:00:00", CultureInfo.InvariantCulture);
                        AppendEscapedSqlString(sqlBuilder, dateTime.ToString("O"));
                    }
                    else if (column.IsBlob())
                    {
                        var blobValue = valElement.GetString()!;
                        sqlBuilder.Append($"zeroblob({blobValue.Length / 2})");
                        blobs?.Add((column, blobValue));
                    }
                    else
                    {
                        AppendEscapedSqlString(sqlBuilder, valElement.GetString() ?? "");
                    }
                    break;
                default:
                    sqlBuilder.Append($"'{valElement.GetRawText()}'");
                    break;
            }
        }

        private static void AppendEscapedSqlString(StringBuilder sql, string text)
        {
            sql.Append('\'');
            foreach (var c in text)
            {
                if (c == '\'') sql.Append(c); // append two '' to escape it in SQL strings
                sql.Append(c);
            }
            sql.Append('\'');
        }

        private static void StoreBlobs(SqliteConnection cn, string tableName, long rowId, List<(ColumnInfo Col, string Val)> blobs)
        {
            if (rowId <= 0) return;

            foreach (var (Col, Val) in blobs)
            {
                using var writeStream = new SqliteBlob(cn, $"{tableName}", $"{Col.Name}", rowId);

                for (int i = 0; i < Val.Length; i += 2)
                {
                    byte b = Convert.ToByte(Val.Substring(i, 2), 16);
                    writeStream.WriteByte(b);
                }

                writeStream.Flush();
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
