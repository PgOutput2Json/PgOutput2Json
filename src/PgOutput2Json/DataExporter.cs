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
    internal static class DataExporter
    {
        public static async Task MaybeExportData(IMessagePublisher publisher,
                                                 IMessageWriter writer,
                                                 ReplicationListenerOptions listenerOptions,
                                                 JsonOptions jsonOptions,
                                                 ILogger? logger,
                                                 CancellationToken token)
        {
            if (!listenerOptions.CopyData)
            {
                return;
            }

            using var connection = new NpgsqlConnection(listenerOptions.ConnectionString);

            await connection.OpenAsync(token).ConfigureAwait(false);

            var publications = await GetPublicationInfo(connection, listenerOptions, token).ConfigureAwait(false);

            var json = new StringBuilder(256);
            var keyVal = new StringBuilder(256);

            foreach (var publication in publications)
            {
                DataCopyStatus dataCopyStatus;
                try
                {
                    dataCopyStatus = await GetDataCopyStatus(publisher, publication, listenerOptions, token).ConfigureAwait(false);

                    if (dataCopyStatus.IsCompleted) continue;
                }
                catch (NotImplementedException)
                {
                    logger.SafeLogWarn("Data copying is not supported in this publisher. Only PgOutput2Json.Sqlite publisher supports it at the moment");
                    break;
                }

                logger.SafeLogInfo("Exporting data from table {TableName} {RowFilter}", publication.TableName, publication.RowFilter);

                await ExportData(connection, publisher, writer, listenerOptions, jsonOptions, publication, dataCopyStatus, json, keyVal, token).ConfigureAwait(false);
            }
        }

        private static async Task<DataCopyStatus> GetDataCopyStatus(IMessagePublisher publisher, PublicationInfo publication, ReplicationListenerOptions listenerOptions, CancellationToken token)
        {
            var dataCopyStatus = await publisher.GetDataCopyStatus(publication.TableName, token).ConfigureAwait(false);

            if (!dataCopyStatus.IsCompleted)
            {
                listenerOptions.DataCopyStatusHandler.Invoke(publication.TableName, dataCopyStatus);
            }

            return dataCopyStatus;
        }

        private static async Task ExportData(NpgsqlConnection connection,
                                             IMessagePublisher publisher,
                                             IMessageWriter writer,
                                             ReplicationListenerOptions listenerOptions,
                                             JsonOptions jsonOptions,
                                             PublicationInfo publication,
                                             DataCopyStatus dataCopyStatus,
                                             StringBuilder json,
                                             StringBuilder keyVal,
                                             CancellationToken token)
        {
            listenerOptions.IncludedColumns.TryGetValue(publication.TableName, out var includedColumns);
            listenerOptions.TablePartitions.TryGetValue(publication.TableName, out var partitionCount);

            // Export two columns to table data
            var selectStatement = $"SELECT * FROM {publication.QuotedTableName}";

            var whereClause = "";

            if (!string.IsNullOrWhiteSpace(publication.RowFilter))
            {
                whereClause = publication.RowFilter;
            }

            if (!string.IsNullOrWhiteSpace(dataCopyStatus.AdditionalRowFilter))
            {
                if (whereClause.Length > 0) whereClause += " AND ";
                whereClause += $"({dataCopyStatus.AdditionalRowFilter})";
            }

            if (whereClause.Length > 0)
            {
                selectStatement += " WHERE " + whereClause;
            }

            if (!string.IsNullOrWhiteSpace(dataCopyStatus.OrderByColumns))
            {
                selectStatement += " ORDER BY " + dataCopyStatus.OrderByColumns;
            }

            using var reader = connection.BeginTextExport($"COPY ({selectStatement}) TO STDOUT");

            var currentBatch = 0;
            var schemaWritten = false;

            string? lastJsonString = null;

            string? line;
            while ((line = await reader.ReadLineAsync(token).ConfigureAwait(false)) != null)
            {
                var values = line.Split('\t');

                if (values.Length != publication.Columns.Count) throw new Exception("Inconsistent column count between COPY output and table metadata");

                json.Clear();
                keyVal.Clear();

                if (currentBatch == 0)
                {
                    currentBatch = listenerOptions.BatchSize;
                }

                json.Append("{\"c\":\"I\"");
                json.Append(",\"w\":0");

                if (jsonOptions.WriteTableNames)
                {
                    json.Append(",\"t\":\"");
                    JsonUtils.EscapeText(json, publication.TableName);
                    json.Append('"');
                }

                if (!schemaWritten)
                {
                    schemaWritten = true;

                    json.Append(',');
                    switch (jsonOptions.WriteMode)
                    {
                        case JsonWriteMode.Compact:
                            WriteSchemaCompact(json, publication, includedColumns);
                            break;
                        default:
                            WriteSchemaDefault(json, publication, includedColumns);
                            break;
                    }
                }

                var hash = WriteRow(publication.Columns, values, json, keyVal, jsonOptions, includedColumns);

                json.Append('}');

                var partition = partitionCount > 0 ? hash % partitionCount : 0;

                lastJsonString = json.ToString();

                await publisher.PublishAsync(0, lastJsonString, publication.TableName, keyVal.ToString(), partition, token).ConfigureAwait(false);

                currentBatch--;
                if (currentBatch <= 0)
                {
                    await publisher.ReportDataCopyProgress(publication.TableName, lastJsonString, token).ConfigureAwait(false);

                    await publisher.ConfirmAsync(token).ConfigureAwait(false);
                }
            }

            await publisher.ReportDataCopyCompleted(publication.TableName, token).ConfigureAwait(false);

            await publisher.ConfirmAsync(token).ConfigureAwait(false);
        }

        private static int WriteRow(IReadOnlyList<ColumnInfo> cols,
                                    string[] values,
                                    StringBuilder json,
                                    StringBuilder keyVal,
                                    JsonOptions jsonOptions,
                                    IReadOnlyList<string>? includedCols)
        {
            int finalHash = 0x12345678;

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
                    if (keyVal.Length == 0)
                    {
                        keyVal.Append('[');
                    }
                    else
                    {
                        keyVal.Append(',');
                    }
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

                    int hash;

                    if (pgOid.IsNumber())
                    {
                        hash = JsonUtils.WriteNumber(json, colValue);
                        if (col.IsKey) JsonUtils.WriteNumber(keyVal, colValue);
                    }
                    else if (pgOid.IsBoolean())
                    {
                        hash = JsonUtils.WriteBoolean(json, colValue);
                        if (col.IsKey) JsonUtils.WriteBoolean(keyVal, colValue);
                    }
                    else if (pgOid.IsByte())
                    {
                        hash = JsonUtils.WriteByte(json, colValue);
                        if (col.IsKey) JsonUtils.WriteByte(keyVal, colValue);
                    }
                    else if (pgOid.IsArrayOfNumber())
                    {
                        hash = JsonUtils.WriteArrayOfNumber(json, colValue);
                        if (col.IsKey) JsonUtils.WriteArrayOfNumber(keyVal, colValue);
                    }
                    else if (pgOid.IsArrayOfByte())
                    {
                        hash = JsonUtils.WriteArrayOfByte(json, colValue);
                        if (col.IsKey) JsonUtils.WriteArrayOfByte(keyVal, colValue);
                    }
                    else if (pgOid.IsArrayOfBoolean())
                    {
                        hash = JsonUtils.WriteArrayOfBoolean(json, colValue);
                        if (col.IsKey) JsonUtils.WriteArrayOfBoolean(keyVal, colValue);
                    }
                    else if (pgOid.IsArrayOfText())
                    {
                        hash = JsonUtils.WriteArrayOfText(json, colValue);
                        if (col.IsKey) JsonUtils.WriteArrayOfText(keyVal, colValue);
                    }
                    else
                    {
                        hash = JsonUtils.WriteText(json, colValue);
                        if (col.IsKey) JsonUtils.WriteText(keyVal, colValue);
                    }

                    if (col.IsKey) finalHash ^= hash;
                }
            }

            if (keyVal.Length > 0 )
            {
                keyVal.Append(']');
            }

            json.Append(jsonOptions.WriteMode == JsonWriteMode.Compact ? ']' : '}');

            return finalHash;
        }

        private static async Task<List<PublicationInfo>> GetPublicationInfo(NpgsqlConnection connection, ReplicationListenerOptions listenerOptions, CancellationToken token)
        {
            var result = new List<PublicationInfo>();

            using (var cmd = connection.CreateCommand())
            {
                var pubNames = string.Join(',', listenerOptions.PublicationNames.Select(x => $"'{x}'"));

                cmd.CommandText = $"SELECT schemaname, tablename, rowfilter FROM pg_catalog.pg_publication_tables WHERE pubname in ({pubNames})";

                using var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);

                while (await reader.ReadAsync(token).ConfigureAwait(false))
                {
                    result.Add(new PublicationInfo
                    {
                        TableName = $"{reader.GetString(0)}.{reader.GetString(1)}",
                        QuotedTableName = $"\"{reader.GetString(0)}\".\"{reader.GetString(1)}\"",
                        RowFilter = reader.IsDBNull(2) ? null : reader.GetString(2),
                    });
                }
            }

            foreach (var publication in result)
            {
                await PopulateColumns(connection, publication, token).ConfigureAwait(false);
            }

            return result;
        }

        private static async Task PopulateColumns(NpgsqlConnection connection, PublicationInfo publication, CancellationToken token)
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
    a.attrelid = '{publication.QuotedTableName}'::regclass
    AND a.attnum > 0
    AND NOT a.attisdropped
ORDER BY
    a.attnum;
";

            using var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);

            while (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                publication.Columns.Add(new ColumnInfo
                {
                    ColumnName = reader.GetString(0),
                    DataTypeId = (uint)reader.GetValue(1),
                    TypeModifier = reader.GetInt32(2),
                    IsKey = reader.GetBoolean(3),
                });
            }
        }

        private static bool IsIncluded(IReadOnlyList<string>? includedCols, ColumnInfo col)
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

        private static void WriteSchemaDefault(StringBuilder json, PublicationInfo publication, IReadOnlyList<string>? includedCols)
        {
            json.Append("\"schema\":{");

            json.Append("\"tableName\":");
            json.Append('"');
            JsonUtils.EscapeText(json, publication.TableName);
            json.Append('"');

            json.Append(",\"columns\":[");

            var i = 0;
            foreach (var col in publication.Columns)
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

        private static void WriteSchemaCompact(StringBuilder json, PublicationInfo pub, IReadOnlyList<string>? includedCols)
        {
            json.Append("\"s\":[");
            json.Append('"');
            JsonUtils.EscapeText(json, pub.TableName);
            json.Append('"');

            foreach (var col in pub.Columns)
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


        private class PublicationInfo
        {
            public required string TableName { get; set; }
            public required string QuotedTableName { get; set; }
            public string? RowFilter { get; set; }

            public List<ColumnInfo> Columns { get; set; } = [];
        }

        private class ColumnInfo
        {
            public required string ColumnName { get; set; }
            public bool IsKey { get; set; }
            public uint DataTypeId { get; set; }
            public int TypeModifier { get; set; }
        }
    }
}
