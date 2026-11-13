using Microsoft.Extensions.Logging;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PgOutput2Json
{
    /// <summary>
    /// Copies the rows of one table, as seen by an exported snapshot, to a publisher.
    /// The rows are published with change type "C" and no position ("w" and "n" are 0).
    /// </summary>
    internal static class TableCopy
    {
        public static async Task<long> CopyAsync(IMessagePublisher publisher,
                                                 ReplicationListenerOptions listenerOptions,
                                                 JsonOptions jsonOptions,
                                                 PublishedTable table,
                                                 string snapshotName,
                                                 ILogger? logger,
                                                 CancellationToken token)
        {
            using var connection = new NpgsqlConnection(listenerOptions.ConnectionString);

            await connection.OpenAsync(token).ConfigureAwait(false);

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = "SET DATESTYLE TO ISO";
                await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);

                // the snapshot of the sync slot: the copy sees exactly the transactions the slot will not send
                cmd.CommandText = $"BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET TRANSACTION SNAPSHOT '{snapshotName.Replace("'", "''")}'";
                await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
            }

            listenerOptions.IncludedColumns.TryGetValue(table.TableName, out var includedColumns);
            listenerOptions.PartitionKeyColumns.TryGetValue(table.TableName, out var partitionKeyCols);

            var columns = await GetColumnsAsync(connection, table, partitionKeyCols, token).ConfigureAwait(false);

            if (columns.Count == 0) throw new Exception($"Table {table.TableName} has no columns to copy");

            // always the SELECT form: it works for a partitioned table too, and it names the columns
            var query = $"SELECT {string.Join(", ", columns.Select(c => TableSyncState.Quote(c.ColumnName)))} FROM {table.QuotedTableName}";

            if (!string.IsNullOrWhiteSpace(table.RowFilter))
            {
                query += $" WHERE {table.RowFilter}";
            }

            var jsonMessage = new JsonMessage { IsTableSync = true };

            jsonMessage.TableName.Append(table.TableName);

            var unconfirmed = 0;
            var totalRows = 0L;

            var value = new StringBuilder(256);
            var values = new List<string>(100);

            using (var reader = await connection.BeginTextExportAsync($"COPY ({query}) TO STDOUT", token).ConfigureAwait(false))
            {
                string? line;
                while ((line = await reader.ReadLineAsync(token).ConfigureAwait(false)) != null)
                {
                    jsonMessage.Json.Clear();
                    jsonMessage.KeyKolValue.Clear();
                    jsonMessage.PartitionKolValue.Clear();

                    GetValues(values, value, line, logger);

                    if (values.Count != columns.Count)
                    {
                        throw new Exception("Inconsistent column count between COPY output and table metadata");
                    }

                    jsonMessage.Json.Append("{\"c\":\"C\"");
                    jsonMessage.Json.Append(",\"w\":0");
                    jsonMessage.Json.Append(",\"n\":0");

                    if (jsonOptions.WriteTableNames)
                    {
                        jsonMessage.Json.Append(",\"t\":\"");
                        JsonUtils.EscapeText(jsonMessage.Json, table.TableName);
                        jsonMessage.Json.Append('"');
                    }

                    if (totalRows == 0)
                    {
                        jsonMessage.Json.Append(',');
                        switch (jsonOptions.WriteMode)
                        {
                            case JsonWriteMode.Compact:
                                WriteSchemaCompact(jsonMessage.Json, table, columns, includedColumns);
                                break;
                            default:
                                WriteSchemaDefault(jsonMessage.Json, table, columns, includedColumns);
                                break;
                        }
                    }

                    WriteRow(columns, values, jsonMessage.Json, jsonMessage.KeyKolValue, jsonMessage.PartitionKolValue, jsonOptions, includedColumns, partitionKeyCols);

                    jsonMessage.Json.Append('}');

                    await publisher.PublishAsync(jsonMessage, token).ConfigureAwait(false);

                    MetricsHelper.IncrementPublishCounter();

                    totalRows++;

                    if (++unconfirmed >= listenerOptions.BatchSize)
                    {
                        await publisher.ConfirmAsync(token).ConfigureAwait(false);
                        unconfirmed = 0;
                    }
                }
            }

            await publisher.ConfirmAsync(token).ConfigureAwait(false);

            return totalRows;
        }

        /// <summary>
        /// Writes row values to the provided values array. Escape sequences are unescaped.
        /// See: https://www.postgresql.org/docs/current/sql-copy.html
        /// </summary>
        internal static void GetValues(List<string> values, StringBuilder value, string line, ILogger? logger)
        {
            values.Clear();
            value.Clear();

            var escape = false;

            foreach (var c in line)
            {
                if (c == '\t')
                {
                    values.Add(value.ToString());
                    value.Clear();
                }
                else if (escape)
                {
                    escape = false;

                    switch (c)
                    {
                        case '\\':
                            value.Append('\\');
                            break;
                        case 'b':
                            value.Append('\b');
                            break;
                        case 'f':
                            value.Append('\f');
                            break;
                        case 'n':
                            value.Append('\n');
                            break;
                        case 'r':
                            value.Append('\r');
                            break;
                        case 't':
                            value.Append('\t');
                            break;
                        case 'v':
                            value.Append('\v');
                            break;
                        case 'N':
                            value.Append('\\');
                            value.Append('N');
                            break;
                        default:
                            logger?.LogWarning("Skipping unknown escape char: \\{char}", c);
                            break;
                    }
                }
                else if (c == '\\')
                {
                    escape = true;
                }
                else
                {
                    value.Append(c);
                }
            }

            // the last value has no terminating tab - and it can be an empty string
            values.Add(value.ToString());
        }

        private static void WriteRow(IReadOnlyList<PgColumnInfo> cols,
                                    List<string> values,
                                    StringBuilder json,
                                    StringBuilder keyVal,
                                    StringBuilder partitionKeyVal,
                                    JsonOptions jsonOptions,
                                    IReadOnlyList<string>? includedCols,
                                    IReadOnlyList<string>? partitionKeyCols)
        {
            json.Append(",\"r\":");
            json.Append(jsonOptions.WriteMode == JsonWriteMode.Compact ? '[' : '{');

            var firstValue = true;

            for (var i = 0; i < cols.Count; i++)
            {
                var colValue = values[i];

                var isNull = colValue == @"\N";

                // we must write nulls in compact mode to preserve the column indexes (no column names)
                if (isNull && !jsonOptions.WriteNulls && jsonOptions.WriteMode != JsonWriteMode.Compact)
                {
                    continue;
                }

                var col = cols[i];

                if (!IsIncluded(includedCols, col))
                {
                    continue;
                }

                if (col.IsKey)
                {
                    keyVal.Append(keyVal.Length == 0 ? '[' : ',');
                }

                if (col.IsPartitionKey)
                {
                    partitionKeyVal.Append(partitionKeyVal.Length == 0 ? '[' : ',');
                }

                if (!firstValue)
                {
                    json.Append(',');
                }

                firstValue = false;

                if (jsonOptions.WriteMode != JsonWriteMode.Compact)
                {
                    // skip property name writing if we are in compact mode

                    json.Append('"');
                    JsonUtils.EscapeText(json, col.ColumnName);
                    json.Append('"');
                    json.Append(':');
                }

                if (isNull)
                {
                    json.Append("null");
                    if (col.IsKey) keyVal.Append("null");
                }
                else
                {
                    var pgOid = (PgOid)col.DataTypeId;

                    if (pgOid.IsNumber())
                    {
                        JsonUtils.WriteNumber(json, colValue);
                        if (col.IsKey) JsonUtils.WriteNumber(keyVal, colValue);
                        if (col.IsPartitionKey) JsonUtils.WriteNumber(partitionKeyVal, colValue);
                    }
                    else if (pgOid.IsBoolean())
                    {
                        JsonUtils.WriteBoolean(json, colValue);
                        if (col.IsKey) JsonUtils.WriteBoolean(keyVal, colValue);
                        if (col.IsPartitionKey) JsonUtils.WriteBoolean(partitionKeyVal, colValue);
                    }
                    else if (pgOid.IsByte())
                    {
                        JsonUtils.WriteByte(json, colValue);
                        if (col.IsKey) JsonUtils.WriteByte(keyVal, colValue);
                        if (col.IsPartitionKey) JsonUtils.WriteByte(partitionKeyVal, colValue);
                    }
                    else if (pgOid.IsArrayOfNumber())
                    {
                        JsonUtils.WriteArrayOfNumber(json, colValue);
                        if (col.IsKey) JsonUtils.WriteArrayOfNumber(keyVal, colValue);
                        if (col.IsPartitionKey) JsonUtils.WriteArrayOfNumber(partitionKeyVal, colValue);
                    }
                    else if (pgOid.IsArrayOfByte())
                    {
                        JsonUtils.WriteArrayOfByte(json, colValue);
                        if (col.IsKey) JsonUtils.WriteArrayOfByte(keyVal, colValue);
                        if (col.IsPartitionKey) JsonUtils.WriteArrayOfByte(partitionKeyVal, colValue);
                    }
                    else if (pgOid.IsArrayOfBoolean())
                    {
                        JsonUtils.WriteArrayOfBoolean(json, colValue);
                        if (col.IsKey) JsonUtils.WriteArrayOfBoolean(keyVal, colValue);
                        if (col.IsPartitionKey) JsonUtils.WriteArrayOfBoolean(partitionKeyVal, colValue);
                    }
                    else if (pgOid.IsArrayOfText())
                    {
                        JsonUtils.WriteArrayOfText(json, colValue);
                        if (col.IsKey) JsonUtils.WriteArrayOfText(keyVal, colValue);
                        if (col.IsPartitionKey) JsonUtils.WriteArrayOfText(partitionKeyVal, colValue);
                    }
                    else
                    {
                        JsonUtils.WriteText(json, colValue);
                        if (col.IsKey) JsonUtils.WriteText(keyVal, colValue);
                        if (col.IsPartitionKey) JsonUtils.WriteText(partitionKeyVal, colValue);
                    }
                }
            }

            if (keyVal.Length > 0)
            {
                keyVal.Append(']');
            }

            if (partitionKeyVal.Length > 0)
            {
                partitionKeyVal.Append(']');
            }

            json.Append(jsonOptions.WriteMode == JsonWriteMode.Compact ? ']' : '}');
        }

        private static async Task<List<PgColumnInfo>> GetColumnsAsync(NpgsqlConnection connection, PublishedTable table, IReadOnlyList<string>? partitionKeyCols, CancellationToken token)
        {
            using var cmd = connection.CreateCommand();

            cmd.CommandText = $@"
SELECT 
    a.attname, 
    a.atttypid, 
    a.atttypmod, 
    CASE WHEN i.indexrelid IS NOT NULL THEN true ELSE false END AS is_key
FROM pg_attribute a
LEFT JOIN
    pg_index i ON i.indrelid = a.attrelid AND i.indisprimary AND a.attnum = ANY(i.indkey)
WHERE
    a.attrelid = {table.Oid}
    AND a.attnum > 0
    AND NOT a.attisdropped
    AND a.attgenerated = ''
ORDER BY
    a.attnum;
";

            var result = new List<PgColumnInfo>();

            using var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);

            while (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                var colName = reader.GetString(0);

                // a publication with a column list publishes only those columns
                if (table.Columns != null && !table.Columns.Contains(colName)) continue;

                result.Add(new PgColumnInfo
                {
                    ColumnName = colName,
                    DataTypeId = (uint)reader.GetValue(1),
                    TypeModifier = reader.GetInt32(2),
                    IsKey = reader.GetBoolean(3),
                    IsPartitionKey = partitionKeyCols != null && partitionKeyCols.Contains(colName),
                });
            }

            return result;
        }

        private static bool IsIncluded(IReadOnlyList<string>? includedCols, PgColumnInfo col)
        {
            if (includedCols == null) return true; // if not specified, included by default

            foreach (var c in includedCols)
            {
                if (c == col.ColumnName)
                {
                    return true;
                }
            }

            return false;
        }

        private static void WriteSchemaDefault(StringBuilder json, PublishedTable table, IReadOnlyList<PgColumnInfo> columns, IReadOnlyList<string>? includedCols)
        {
            json.Append("\"schema\":{");

            json.Append("\"tableName\":");
            json.Append('"');
            JsonUtils.EscapeText(json, table.TableName);
            json.Append('"');

            json.Append(",\"columns\":[");

            var i = 0;
            foreach (var col in columns)
            {
                if (!IsIncluded(includedCols, col)) continue;

                if (i > 0) json.Append(',');

                json.Append('{');

                json.Append("\"name\":");
                json.Append('"');
                JsonUtils.EscapeText(json, col.ColumnName);
                json.Append('"');

                json.Append(",\"isKey\":");
                json.Append(col.IsKey ? "true" : "false");

                json.Append(",\"dataType\":");
                JsonUtils.EscapeText(json, col.DataTypeId.ToString());

                if (col.TypeModifier != -1)
                {
                    json.Append(",\"typeModifier\":");
                    JsonUtils.EscapeText(json, col.TypeModifier.ToString());
                }

                json.Append('}');
                i++;
            }

            json.Append(']'); // columns
            json.Append('}'); // schema
        }

        private static void WriteSchemaCompact(StringBuilder json, PublishedTable table, IReadOnlyList<PgColumnInfo> columns, IReadOnlyList<string>? includedCols)
        {
            json.Append("\"s\":[");
            json.Append('"');
            JsonUtils.EscapeText(json, table.TableName);
            json.Append('"');

            foreach (var col in columns)
            {
                if (!IsIncluded(includedCols, col)) continue;

                json.Append(',');
                json.Append('[');

                json.Append('"');
                JsonUtils.EscapeText(json, col.ColumnName);
                json.Append('"');
                json.Append(',');

                json.Append(col.IsKey ? '1' : '0');
                json.Append(',');

                JsonUtils.EscapeText(json, col.DataTypeId.ToString());

                if (col.TypeModifier != -1)
                {
                    json.Append(',');
                    JsonUtils.EscapeText(json, col.TypeModifier.ToString());
                }

                json.Append(']');
            }

            json.Append(']');
        }

    }
}
