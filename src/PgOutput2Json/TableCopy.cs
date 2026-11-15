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
    /// How far a copy has come: the rows of the chunks the publisher has confirmed. A copy that fails
    /// continues from here, as long as the snapshot can still be imported.
    /// </summary>
    internal sealed class CopyProgress
    {
        /// <summary>
        /// The relations copied completely: the table, or the leaf partitions of a partitioned table. By oid, because
        /// the partitions are read from the current catalog, and one can be attached or detached between two attempts.
        /// </summary>
        public HashSet<uint> CopiedRelations { get; } = [];

        /// <summary>The relation being copied, and its first page that is not copied yet.</summary>
        public uint Relation { get; set; }
        public long Page { get; set; }

        public long Rows { get; set; }
    }

    /// <summary>
    /// The snapshot of a copy could not be imported: it is gone with the connection that exported it,
    /// and the copy has to start again from zero.
    /// </summary>
    internal sealed class SnapshotUnavailableException(string message, Exception inner) : Exception(message, inner);

    /// <summary>
    /// Copies the rows of one table, as seen by an exported snapshot, to a publisher.
    /// The rows are published with change type "C" and no position ("w" and "n" are 0).
    /// </summary>
    internal static class TableCopy
    {
        /// <summary>
        /// The pages copied by one COPY: 8 MB with the default block size. After an error the copy continues
        /// with the chunk it was in, so a chunk is what is published twice.
        /// </summary>
        internal static long PagesPerChunk { get; set; } = 1000;

        public static async Task<long> CopyAsync(IMessagePublisher publisher,
                                                 ReplicationListenerOptions listenerOptions,
                                                 NpgsqlDataSource dataSource,
                                                 JsonOptions jsonOptions,
                                                 PublishedTable table,
                                                 string snapshotName,
                                                 CopyProgress progress,
                                                 ILogger? logger,
                                                 CancellationToken token)
        {
            using var connection = dataSource.CreateConnection();

            try
            {
                await connection.OpenAsync(token).ConfigureAwait(false);

                using var cmd = connection.CreateCommand();

                cmd.CommandText = "SET DATESTYLE TO ISO";
                await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);

                // the snapshot of the sync slot: the copy sees exactly the transactions the slot will not send
                cmd.CommandText = $"BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET TRANSACTION SNAPSHOT '{snapshotName.Replace("'", "''")}'";
                await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
            }
            catch (Exception ex) when (!token.IsCancellationRequested)
            {
                throw new SnapshotUnavailableException($"Could not import the snapshot of the copy of {table.TableName}", ex);
            }

            listenerOptions.IncludedColumns.TryGetValue(table.TableName, out var includedColumns);
            listenerOptions.PartitionKeyColumns.TryGetValue(table.TableName, out var partitionKeyCols);

            var columns = await GetColumnsAsync(connection, table, partitionKeyCols, token).ConfigureAwait(false);

            if (columns.Count == 0) throw new Exception($"Table {table.TableName} has no columns to copy");

            var relations = await GetRelationsAsync(connection, table, token).ConfigureAwait(false);

            // the SELECT form names the columns; ONLY, because a child of an inheritance tree is a table of its own
            // in the publication, and the leaves of a partitioned table are copied one by one
            var select = $"SELECT {string.Join(", ", columns.Select(c => TableSyncState.Quote(c.ColumnName)))} FROM ONLY ";

            var rowFilter = string.IsNullOrWhiteSpace(table.RowFilter) ? "" : $" AND ({table.RowFilter})";

            var jsonMessage = new JsonMessage { IsTableSync = true };

            jsonMessage.TableName.Append(table.TableName);

            // a new publisher after an error needs the schema again
            var schemaWritten = false;

            var value = new StringBuilder(256);
            var values = new List<string>(100);

            foreach (var (oid, relationName, pages) in relations)
            {
                if (progress.CopiedRelations.Contains(oid)) continue;

                if (progress.Relation != oid)
                {
                    progress.Relation = oid;
                    progress.Page = 0;
                }

                while (true)
                {
                    // a range of pages, read directly by a TID range scan; the last one is open, so nothing is missed
                    var last = progress.Page + PagesPerChunk >= pages;

                    var query = $"{select}{relationName} WHERE ctid >= '({progress.Page},0)'"
                        + (last ? "" : $" AND ctid < '({progress.Page + PagesPerChunk},0)'")
                        + rowFilter;

                    var unconfirmed = 0;
                    var chunkRows = 0L;

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

                            if (!schemaWritten)
                            {
                                schemaWritten = true;

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

                            chunkRows++;

                            if (++unconfirmed >= listenerOptions.BatchSize)
                            {
                                await publisher.ConfirmAsync(token).ConfigureAwait(false);
                                unconfirmed = 0;
                            }
                        }
                    }

                    // the chunk is done once the publisher has confirmed all of it
                    await publisher.ConfirmAsync(token).ConfigureAwait(false);

                    progress.Rows += chunkRows;

                    if (last) break;

                    progress.Page += PagesPerChunk;
                }

                progress.CopiedRelations.Add(oid);
            }

            return progress.Rows;
        }

        /// <summary>
        /// The relations that hold the rows of the table, with their number of pages: the table itself,
        /// or the leaf partitions of a partitioned table. No row the snapshot sees is in a page added after it.
        /// </summary>
        private static async Task<List<(uint Oid, string Name, long Pages)>> GetRelationsAsync(NpgsqlConnection connection, PublishedTable table, CancellationToken token)
        {
            using var cmd = connection.CreateCommand();

            cmd.CommandText = $@"
SELECT c.oid, format('%I.%I', n.nspname, c.relname), pg_relation_size(c.oid) / current_setting('block_size')::bigint
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.oid = {table.Oid} AND c.relkind <> 'p'
   OR c.oid IN (SELECT relid FROM pg_partition_tree({table.Oid}::oid::regclass) WHERE isleaf) AND c.relkind = 'r'
ORDER BY c.oid";

            var result = new List<(uint, string, long)>();

            using var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);

            while (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                result.Add(((uint)reader.GetValue(0), reader.GetString(1), reader.GetInt64(2)));
            }

            return result;
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

            // the key columns are those of the replica identity, as in the stream: the primary key by default,
            // the columns of the replica identity index, every column for FULL, and none for NOTHING
            cmd.CommandText = $@"
SELECT 
    a.attname, 
    a.atttypid, 
    a.atttypmod, 
    CASE c.relreplident
        WHEN 'f' THEN true
        WHEN 'n' THEN false
        ELSE EXISTS (
            SELECT 1 FROM pg_index i
            WHERE i.indrelid = a.attrelid
              AND (CASE c.relreplident WHEN 'i' THEN i.indisreplident ELSE i.indisprimary END)
              AND a.attnum = ANY(i.indkey))
    END AS is_key,
    -- the name pgoutput sends for a type that is not built in: that of the base type, for a domain
    CASE WHEN a.atttypid >= 10000 THEN (
        WITH RECURSIVE base AS (
            SELECT t.typname, t.typtype, t.typbasetype FROM pg_type t WHERE t.oid = a.atttypid
            UNION ALL
            SELECT t.typname, t.typtype, t.typbasetype FROM pg_type t JOIN base b ON t.oid = b.typbasetype WHERE b.typtype = 'd')
        SELECT typname::text FROM base WHERE typtype <> 'd')
    END AS type_name
FROM pg_attribute a
JOIN pg_class c ON c.oid = a.attrelid
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
                    TypeName = reader.GetValue(4) as string,
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

                if (col.TypeName != null)
                {
                    json.Append(",\"typeName\":\"");
                    JsonUtils.EscapeText(json, col.TypeName);
                    json.Append('"');
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

                if (col.TypeModifier != -1 || col.TypeName != null)
                {
                    json.Append(',');
                    JsonUtils.EscapeText(json, col.TypeModifier.ToString());
                }

                if (col.TypeName != null)
                {
                    json.Append(",\"");
                    JsonUtils.EscapeText(json, col.TypeName);
                    json.Append('"');
                }

                json.Append(']');
            }

            json.Append(']');
        }

    }
}
