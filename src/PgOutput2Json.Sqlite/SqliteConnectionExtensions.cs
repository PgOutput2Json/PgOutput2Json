using System;
using System.Collections.Generic;
using System.Globalization;
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
        public static async Task<ulong> GetWalEnd(this SqliteConnection cn, CancellationToken token)
        {
            var cfgValue = await GetConfig(cn, ConfigKey.WalEnd, token).ConfigureAwait(false);
            
            return cfgValue != null ? ulong.Parse(cfgValue, CultureInfo.InvariantCulture) : 0;
        }

        public static async Task SetWalEnd(this SqliteConnection cn, ulong walEnd, CancellationToken token)
        {
            await SaveConfig(cn, ConfigKey.WalEnd, walEnd.ToString(CultureInfo.InvariantCulture), token).ConfigureAwait(false);
        }

        public static async Task<string?> GetDataCopyProgress(this SqliteConnection cn, string tableName, CancellationToken token)
        {
            return await GetConfig(cn, $"{ConfigKey.DataCopyProgress}_{tableName}", token).ConfigureAwait(false);
        }

        public static async Task SetDataCopyProgress(this SqliteConnection cn, string tableName, string lastJson, CancellationToken token)
        {
            await SaveConfig(cn, $"{ConfigKey.DataCopyProgress}_{tableName}", lastJson, token).ConfigureAwait(false);
        }

        public static async Task SetDataCopyCompleted(this SqliteConnection cn, string tableName, CancellationToken token)
        {
            await SaveConfig(cn, $"{ConfigKey.DataCopyProgress}_{tableName}", ConfigKey.DataCopyProgressCompleted, token).ConfigureAwait(false);
        }

        public static bool IsDataCopyCompleted(this string? cfgValue)
        {
            return cfgValue != null && cfgValue == ConfigKey.DataCopyProgressCompleted;
        }

        public static async Task SaveConfig(this SqliteConnection cn, string key, string value, CancellationToken token)
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

        public static async Task<string?> GetConfig(this SqliteConnection cn, string key, CancellationToken token)
        {
            using var cmd = cn.CreateCommand();

            cmd.Parameters.AddWithValue("cfg_key", key);

            cmd.CommandText = "SELECT cfg_value FROM __pg2j_config WHERE cfg_key = @cfg_key";

            var result = await cmd.ExecuteScalarAsync(token).ConfigureAwait(false);

            return result?.ToString();
        }

        public static async Task CreateConfigTable(this SqliteConnection cn, CancellationToken token)
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

        public static async Task TryCreateTable(this SqliteConnection cn, string fullTableName, IReadOnlyList<ColumnInfo> columns, CancellationToken token)
        {
            var sqlBuilder = new StringBuilder(256);
            var keyBuilder = new StringBuilder(256);

            string tableName = GetTableName(fullTableName);

            sqlBuilder.Append($"CREATE TABLE IF NOT EXISTS \"{tableName}\" (");

            var i = 0;
            foreach (var colInfo in columns)
            {
                if (i > 0) sqlBuilder.Append(", ");
                sqlBuilder.Append($"\"{colInfo.Name}\" {colInfo.GetSqliteType()}");

                if (colInfo.IsKey)
                {
                    if (keyBuilder.Length > 0) keyBuilder.Append(", ");
                    keyBuilder.Append(colInfo.Name);
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

        public static async Task UpdateOrInsert(this SqliteConnection cn,
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
                try
                {
                    await cn.Insert(fullTableName, columns, rowElement, token).ConfigureAwait(false);
                }
                catch (SqliteException ex)
                {
                    if (!ex.IsPrimaryKeyViolation())
                    {
                        throw;
                    }

                    if (logger != null && logger.IsEnabled(LogLevel.Warning))
                    {
                        logger.LogWarning("Skipping existing row for table {TableName}", fullTableName);
                    }
                }
            }
            else if (changeType == "U")
            {
                var count = await cn.Update(fullTableName, columns, keyElement, rowElement, token).ConfigureAwait(false);
                if (count == 0)
                {
                    await cn.Insert(fullTableName, columns, rowElement, token).ConfigureAwait(false);
                }
            }
            else if (changeType == "D")
            {
                await cn.Delete(fullTableName, columns, keyElement, token).ConfigureAwait(false);
            }

            await cn.SetWalEnd(walEnd, token).ConfigureAwait(false);
        }

        public static async Task Insert(this SqliteConnection cn, string fullTableName, IReadOnlyList<ColumnInfo> columns, JsonElement rowElement, CancellationToken token)
        {
            var tableName = GetTableName(fullTableName);

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

                WriteColumnValue(sqlBuilder, valElement);

                i++;
            }

            sqlBuilder.Append(')');

            using var cmd = cn.CreateCommand();

            cmd.CommandText = sqlBuilder.ToString();

            await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }

        public static async Task<int> Update(this SqliteConnection cn, string fullTableName, IReadOnlyList<ColumnInfo> columns, JsonElement keyElement, JsonElement rowElement, CancellationToken token)
        {
            var tableName = GetTableName(fullTableName);

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

                    WriteColumnValueAssignment(whereBuilder, keyElement[i], column, true);

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

                    WriteColumnValueAssignment(whereBuilder, rowElement[i], column);

                    i++;
                    continue;
                } 

                if (setFieldsBuilder.Length > 0) setFieldsBuilder.Append(", ");

                WriteColumnValueAssignment(setFieldsBuilder, rowElement[i], column);

                i++;
            }

            var sqlBuilder = new StringBuilder($"UPDATE \"{tableName}\" SET ");
            sqlBuilder.Append(setFieldsBuilder);
            sqlBuilder.Append(" WHERE ");
            sqlBuilder.Append(whereBuilder);

            using var cmd = cn.CreateCommand();

            cmd.CommandText = sqlBuilder.ToString();

            return await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }

        public static async Task Delete(this SqliteConnection cn, string fullTableName, IReadOnlyList<ColumnInfo> columns, JsonElement keyElement, CancellationToken token)
        {
            var tableName = GetTableName(fullTableName);

            var sqlBuilder = new StringBuilder($"DELETE FROM \"{tableName}\" WHERE ");

            var i = 0;

            foreach (var column in columns)
            {
                if (!column.IsKey) continue;

                if (i > 0) sqlBuilder.Append(" AND ");

                WriteColumnValueAssignment(sqlBuilder, keyElement[i], column, true);

                i++;
            }

            using var cmd = cn.CreateCommand();

            cmd.CommandText = sqlBuilder.ToString();

            await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }

        private static void WriteColumnValueAssignment(StringBuilder keysBuilder, JsonElement rowElement, ColumnInfo column, bool isWhereStatement = false)
        {
            if (isWhereStatement && rowElement.ValueKind == JsonValueKind.Null)
            {
                keysBuilder.Append($"\"{column.Name}\" IS NULL");
            }
            else
            {
                keysBuilder.Append($"\"{column.Name}\"=");
                WriteColumnValue(keysBuilder, rowElement);
            }
        }

        private static void WriteColumnValue(StringBuilder sqlBuilder, JsonElement valElement)
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
                    sqlBuilder.Append($"'{valElement.GetString()}'");
                    break;
                default:
                    sqlBuilder.Append($"'{valElement.GetRawText()}'");
                    break;
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
                _ => "TEXT",// Default fallback for unknown types
            };
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
