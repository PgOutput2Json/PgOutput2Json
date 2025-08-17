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
        private const string _finalMarker = "__FINAL__";

        public static async Task MaybeExportDataAsync(IMessagePublisherFactory publisherFactory,
                                                      ReplicationListenerOptions listenerOptions,
                                                      JsonOptions jsonOptions,
                                                      string slotName,
                                                      ILoggerFactory? loggerFactory,
                                                      CancellationToken token)
        {
            if (!listenerOptions.CopyData)
            {
                return;
            }

            await DataCopyProgress.CreateDataCopyProgressTableAsync(listenerOptions, token).ConfigureAwait(false);

            var finalCopyStatus = await DataCopyProgress.GetDataCopyStatusAsync(_finalMarker, listenerOptions, token).ConfigureAwait(false);
            if (finalCopyStatus.IsCompleted) return;

            List<PublicationInfo> publications;

            using (var connection = new NpgsqlConnection(listenerOptions.ConnectionString))
            {
                await connection.OpenAsync(token).ConfigureAwait(false);

                publications = await GetPublicationInfoAsync(connection, listenerOptions, token).ConfigureAwait(false);
            }

            bool hasErrors = false;

            using var cts = new CancellationTokenSource();

            var parallelOptions = new ParallelOptions
            {
                CancellationToken = CancellationTokenSource.CreateLinkedTokenSource(token).Token,
                MaxDegreeOfParallelism = listenerOptions.MaxParallelCopyJobs,
            };

            await Parallel.ForEachAsync(publications, parallelOptions, async (publication, linkedToken) =>
            {
                var logger = loggerFactory?.CreateLogger<ReplicationListener>();

                try
                {
                    await using var publisher = publisherFactory.CreateMessagePublisher(listenerOptions, slotName, loggerFactory);

                    using var connection = new NpgsqlConnection(listenerOptions.ConnectionString);

                    await connection.OpenAsync(linkedToken).ConfigureAwait(false);

                    var dataCopyStatus = await DataCopyProgress.GetDataCopyStatusAsync(publication.TableName, listenerOptions, linkedToken).ConfigureAwait(false);

                    if (dataCopyStatus.IsCompleted)
                    {
                        return;
                    }

                    logger.SafeLogInfo("Exporting data from Table: {TableName}, RowFilter: {RowFilter}", publication.TableName, publication.RowFilter);

                    while (!linkedToken.IsCancellationRequested)
                    {
                        var completed = await ExportDataAsync(connection, publisher, listenerOptions, jsonOptions, publication, dataCopyStatus, logger, linkedToken).ConfigureAwait(false);
                        if (completed) break;

                        // needed for continuation of the next batch
                        dataCopyStatus = await DataCopyProgress.GetDataCopyStatusAsync(publication.TableName, listenerOptions, linkedToken).ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException)
                {
                    logger.SafeLogWarn("Cancelling data export from table {TableName}", publication.TableName);
                }
                catch (Exception ex)
                {
                    hasErrors = true;

                    logger.SafeLogError(ex, "Error in data export from table {TableName}", publication.TableName);

                    await cts.CancelAsync().ConfigureAwait(false);
                }
            }).ConfigureAwait(false);

            if (hasErrors)
            {
                throw new OperationCanceledException(); // cancel further processing (errors already logged)
            }

            await DataCopyProgress.SetDataCopyProgressAsync(_finalMarker, listenerOptions, true, null, null, token).ConfigureAwait(false);
        }

        private static async Task<bool> ExportDataAsync(NpgsqlConnection connection,
                                                        IMessagePublisher publisher,
                                                        ReplicationListenerOptions listenerOptions,
                                                        JsonOptions jsonOptions,
                                                        PublicationInfo publication,
                                                        DataCopyStatus dataCopyStatus,
                                                        ILogger? logger,
                                                        CancellationToken cancellationToken)
        {
            listenerOptions.IncludedColumns.TryGetValue(publication.TableName, out var includedColumns);
            listenerOptions.TablePartitions.TryGetValue(publication.TableName, out var partitionCount);

            // Export two columns to table data

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

            var source = publication.QuotedTableName;
            var copyBatchSize = 0;

            if (whereClause.Length != 0 || !string.IsNullOrWhiteSpace(dataCopyStatus.OrderByColumns))
            {
                source = $"(SELECT * FROM {publication.QuotedTableName}";

                if (whereClause.Length > 0)
                {
                    source += " WHERE " + whereClause;
                }

                if (!string.IsNullOrWhiteSpace(dataCopyStatus.OrderByColumns))
                {
                    source += " ORDER BY " + dataCopyStatus.OrderByColumns;

                    if (listenerOptions.CopyDataBatchSize > 0)
                    {
                        source += " LIMIT " + listenerOptions.CopyDataBatchSize;
                        copyBatchSize = listenerOptions.CopyDataBatchSize;
                    }
                }

                source += ")";
            }

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = "SET DATESTYLE TO ISO";
                await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            var jsonMessage = new JsonMessage();

            jsonMessage.TableName.Append(publication.TableName);

            var currentBatch = 0;
            var totalRows = 0;
            var schemaWritten = false;

            var value = new StringBuilder(256);
            var values = new List<string>(100);

            using var reader = connection.BeginTextExport($"COPY {source} TO STDOUT HEADER");

            var isHeader = true;

            var token = CancellationToken.None; // we will check cancellation manually after each line

            string? line;
            while ((line = await reader.ReadLineAsync(token).ConfigureAwait(false)) != null)
            {
                if (isHeader)
                {
                    isHeader = false;
                    GetValues(values, value, line, logger);

                    var columnsFromHeader = new List<PgColumnInfo>();

                    for (var i = 0; i < values.Count; i++)
                    {
                        var col = publication.Columns.FirstOrDefault(c => c.ColumnName == values[i])
                            ?? throw new Exception($"Invalid column in exported data. Column: {values[i]}");

                        columnsFromHeader.Add(col);
                    }

                    // use columns from the header
                    publication.Columns = columnsFromHeader;
                    continue;
                }

                // we check here, to make sure header is read at least

                if (cancellationToken.IsCancellationRequested) break;

                jsonMessage.Json.Clear();
                jsonMessage.KeyKolValue.Clear();

                GetValues(values, value, line, logger);

                if (values.Count != publication.Columns.Count)
                {
                    throw new Exception("Inconsistent column count between COPY output and table metadata");
                }

                if (currentBatch == 0)
                {
                    currentBatch = listenerOptions.BatchSize;
                }

                jsonMessage.Json.Append("{\"c\":\"I\"");
                jsonMessage.Json.Append(",\"w\":0");

                if (jsonOptions.WriteTableNames)
                {
                    jsonMessage.Json.Append(",\"t\":\"");
                    JsonUtils.EscapeText(jsonMessage.Json, publication.TableName);
                    jsonMessage.Json.Append('"');
                }

                if (!schemaWritten)
                {
                    schemaWritten = true;

                    jsonMessage.Json.Append(',');
                    switch (jsonOptions.WriteMode)
                    {
                        case JsonWriteMode.Compact:
                            WriteSchemaCompact(jsonMessage.Json, publication, includedColumns);
                            break;
                        default:
                            WriteSchemaDefault(jsonMessage.Json, publication, includedColumns);
                            break;
                    }
                }

                var hash = WriteRow(publication.Columns, values, jsonMessage.Json, jsonMessage.KeyKolValue, jsonOptions, includedColumns);

                jsonMessage.Json.Append('}');

                var partition = partitionCount > 0 ? hash % partitionCount : 0;

                await publisher.PublishAsync(jsonMessage, token).ConfigureAwait(false);

                totalRows++;

                currentBatch--;
                if (currentBatch <= 0)
                {
                    await publisher.ConfirmAsync(token).ConfigureAwait(false);
                    await DataCopyProgress.SetDataCopyProgressAsync(publication.TableName, listenerOptions, false, jsonMessage.Json.ToString(), publication.Columns, token).ConfigureAwait(false);
                }
            }

            var completed = !cancellationToken.IsCancellationRequested 
                && (copyBatchSize == 0 || totalRows < copyBatchSize);

            await publisher.ConfirmAsync(token).ConfigureAwait(false);
            await DataCopyProgress.SetDataCopyProgressAsync(publication.TableName, listenerOptions, completed, jsonMessage.Json.ToString(), publication.Columns, token).ConfigureAwait(false);

            return completed;
        }

        /// <summary>
        /// Writes row values to the provided values array. Escape sequences are unescaped.
        /// See: https://www.postgresql.org/docs/current/sql-copy.html
        /// </summary>
        /// <param name="values"></param>
        /// <param name="value"></param>
        /// <param name="line"></param>
        /// <param name="logger"></param>
        /// <exception cref="Exception"></exception>
        private static void GetValues(List<string> values, StringBuilder value, string line, ILogger? logger)
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

            if (value.Length > 0)
            {
                values.Add(value.ToString());
            }
        }

        private static int WriteRow(IReadOnlyList<PgColumnInfo> cols,
                                    List<string> values,
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

        private static async Task<List<PublicationInfo>> GetPublicationInfoAsync(NpgsqlConnection connection, ReplicationListenerOptions listenerOptions, CancellationToken token)
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
                        RowFilter = await reader.IsDBNullAsync(2, token).ConfigureAwait(false) ? null : reader.GetString(2),
                    });
                }
            }

            foreach (var publication in result)
            {
                await PopulateColumnsAsync(connection, publication, token).ConfigureAwait(false);
            }

            return result;
        }

        private static async Task PopulateColumnsAsync(NpgsqlConnection connection, PublicationInfo publication, CancellationToken token)
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
    AND a.attgenerated = ''
ORDER BY
    a.attnum;
";

            using var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);

            while (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                publication.Columns.Add(new PgColumnInfo
                {
                    ColumnName = reader.GetString(0),
                    DataTypeId = (uint)reader.GetValue(1),
                    TypeModifier = reader.GetInt32(2),
                    IsKey = reader.GetBoolean(3),
                });
            }
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

            public List<PgColumnInfo> Columns { get; set; } = [];
        }
    }
}
