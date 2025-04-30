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
        public static async Task MaybeExportData(IMessagePublisherFactory publisherFactory,
                                                 IMessageWriter writer,
                                                 ReplicationListenerOptions listenerOptions,
                                                 JsonOptions jsonOptions,
                                                 ILoggerFactory? loggerFactory,
                                                 CancellationToken token)
        {
            if (!listenerOptions.CopyData)
            {
                return;
            }

            await DataCopyProgress.CreateDataCopyProgressTable(listenerOptions, token).ConfigureAwait(false);

            List<PublicationInfo> publications;

            using (var connection = new NpgsqlConnection(listenerOptions.ConnectionString))
            {
                await connection.OpenAsync(token).ConfigureAwait(false);

                publications = await GetPublicationInfo(connection, listenerOptions, token).ConfigureAwait(false);
            }

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
                    await using var publisher = publisherFactory.CreateMessagePublisher(listenerOptions, loggerFactory);

                    using var connection = new NpgsqlConnection(listenerOptions.ConnectionString);

                    await connection.OpenAsync(linkedToken).ConfigureAwait(false);

                    var dataCopyStatus = await DataCopyProgress.GetDataCopyStatus(publication.TableName, listenerOptions, linkedToken).ConfigureAwait(false);

                    if (!dataCopyStatus.IsCompleted)
                    {
                        logger.SafeLogInfo("Exporting data from Table: {TableName}, RowFilter: {RowFilter}", publication.TableName, publication.RowFilter);

                        await ExportData(connection, publisher, writer, listenerOptions, jsonOptions, publication, dataCopyStatus, logger, linkedToken).ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException)
                {
                    logger.SafeLogWarn("Cancelling data export from table {TableName}", publication.TableName);
                }
                catch (Exception ex)
                {
                    logger.SafeLogError(ex, "Error while exporting data");
                    cts.Cancel();
                }
            });
        }

        private static async Task ExportData(NpgsqlConnection connection,
                                             IMessagePublisher publisher,
                                             IMessageWriter writer,
                                             ReplicationListenerOptions listenerOptions,
                                             JsonOptions jsonOptions,
                                             PublicationInfo publication,
                                             DataCopyStatus dataCopyStatus,
                                             ILogger? logger,
                                             CancellationToken cancellationToken)
        {
            string? lastJsonString = null;

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
                }

                source += ")";
            }

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = "SET DATESTYLE TO ISO";
                await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            var json = new StringBuilder(256);
            var keyValues = new StringBuilder(256);
            var value = new StringBuilder(256);

            var currentBatch = 0;
            var schemaWritten = false;

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

                json.Clear();
                keyValues.Clear();

                GetValues(values, value, line, logger);

                if (values.Count != publication.Columns.Count)
                {
                    throw new Exception("Inconsistent column count between COPY output and table metadata");
                }

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

                var hash = WriteRow(publication.Columns, values, json, keyValues, jsonOptions, includedColumns);

                json.Append('}');

                var partition = partitionCount > 0 ? hash % partitionCount : 0;

                lastJsonString = json.ToString();

                await publisher.PublishAsync(0, lastJsonString, publication.TableName, keyValues.ToString(), partition, token).ConfigureAwait(false);

                currentBatch--;
                if (currentBatch <= 0)
                {
                    await publisher.ConfirmAsync(token).ConfigureAwait(false);
                    await DataCopyProgress.SetDataCopyProgress(publication.TableName, listenerOptions, false, lastJsonString, publication.Columns, token).ConfigureAwait(false);
                }
            }

            var completed = !cancellationToken.IsCancellationRequested;

            await publisher.ConfirmAsync(token).ConfigureAwait(false);
            await DataCopyProgress.SetDataCopyProgress(publication.TableName, listenerOptions, completed, lastJsonString, publication.Columns, token).ConfigureAwait(false);
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
