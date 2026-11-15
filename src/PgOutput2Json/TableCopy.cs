using Microsoft.Extensions.Logging;
using Npgsql;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PgOutput2Json
{
    /// <summary>
    /// How far a copy has come: the chunks the publishers have confirmed. A copy that fails continues
    /// with the other chunks, as long as the snapshot can still be imported. Shared by the workers of a copy.
    /// </summary>
    internal sealed class CopyProgress
    {
        // by the oid of the relation, because the partitions are read from the current catalog,
        // and one can be attached or detached between two attempts
        private readonly HashSet<(uint Relation, long Page)> _copiedChunks = [];

        private long _rows;

        public long Rows => Interlocked.Read(ref _rows);

        public bool IsCopied(uint relation, long page)
        {
            lock (_copiedChunks) return _copiedChunks.Contains((relation, page));
        }

        public void SetCopied(uint relation, long page, long rows)
        {
            lock (_copiedChunks) _copiedChunks.Add((relation, page));

            Interlocked.Add(ref _rows, rows);
        }
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
        /// with the chunks that were not confirmed, so a chunk is what is published twice.
        /// </summary>
        internal static long PagesPerChunk { get; set; } = 1000;

        /// <summary>
        /// A range of pages of one relation, read directly by a TID range scan. The last one of a relation is open,
        /// so no row is missed if the relation has grown.
        /// </summary>
        private sealed record Chunk(uint Relation, string RelationName, long Page, bool IsLast);

        /// <summary>
        /// What every worker of a copy needs.
        /// </summary>
        private sealed record CopyContext(PublishedTable Table,
                                          List<PgColumnInfo> Columns,
                                          string Select,
                                          string RowFilter,
                                          IReadOnlyList<string>? IncludedColumns,
                                          IReadOnlyList<string>? PartitionKeyColumns,
                                          ReplicationListenerOptions ListenerOptions,
                                          JsonOptions JsonOptions,
                                          ConcurrentQueue<Chunk> Chunks,
                                          CopyProgress Progress,
                                          ILogger? Logger);

        /// <summary>
        /// Copies the chunks that are not copied yet, with up to <paramref name="workers"/> workers that take
        /// the next chunk when they are done with one. The first worker uses <paramref name="publisher"/>,
        /// each other one a connection and a publisher of its own, in the same snapshot.
        /// </summary>
        public static async Task<long> CopyAsync(IMessagePublisher publisher,
                                                 Func<IMessagePublisher> createPublisher,
                                                 int workers,
                                                 ReplicationListenerOptions listenerOptions,
                                                 NpgsqlDataSource dataSource,
                                                 JsonOptions jsonOptions,
                                                 PublishedTable table,
                                                 string snapshotName,
                                                 CopyProgress progress,
                                                 ILogger? logger,
                                                 CancellationToken token)
        {
            using var connection = await ImportSnapshotAsync(dataSource, table, snapshotName, token).ConfigureAwait(false);

            listenerOptions.IncludedColumns.TryGetValue(table.TableName, out var includedColumns);
            listenerOptions.PartitionKeyColumns.TryGetValue(table.TableName, out var partitionKeyCols);

            var columns = await GetColumnsAsync(connection, table, partitionKeyCols, token).ConfigureAwait(false);

            if (columns.Count == 0) throw new Exception($"Table {table.TableName} has no columns to copy");

            var chunks = new ConcurrentQueue<Chunk>();

            foreach (var (oid, relationName, pages) in await GetRelationsAsync(connection, table, token).ConfigureAwait(false))
            {
                for (var page = 0L; ; page += PagesPerChunk)
                {
                    var isLast = page + PagesPerChunk >= pages;

                    if (!progress.IsCopied(oid, page)) chunks.Enqueue(new Chunk(oid, relationName, page, isLast));

                    if (isLast) break;
                }
            }

            // the SELECT form names the columns; ONLY, because a child of an inheritance tree is a table of its own
            // in the publication, and the leaves of a partitioned table are copied one by one
            var context = new CopyContext(table,
                                          columns,
                                          $"SELECT {string.Join(", ", columns.Select(c => TableSyncState.Quote(c.ColumnName)))} FROM ONLY ",
                                          string.IsNullOrWhiteSpace(table.RowFilter) ? "" : $" AND ({table.RowFilter})",
                                          includedColumns,
                                          partitionKeyCols,
                                          listenerOptions,
                                          jsonOptions,
                                          chunks,
                                          progress,
                                          logger);

            // the others stop when one fails: the chunks they did not finish are copied again by the next attempt
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(token);

            async Task RunAsync(Func<Task> work)
            {
                try
                {
                    await work().ConfigureAwait(false);
                }
                catch
                {
                    await cts.CancelAsync().ConfigureAwait(false);
                    throw;
                }
            }

            async Task RunOtherWorkerAsync()
            {
                using var otherConnection = await ImportSnapshotAsync(dataSource, table, snapshotName, cts.Token).ConfigureAwait(false);

                var otherPublisher = createPublisher();

                await using var publisherScope = otherPublisher.ConfigureAwait(false);

                await CopyChunksAsync(otherConnection, otherPublisher, context, cts.Token).ConfigureAwait(false);
            }

            var tasks = new List<Task> { RunAsync(() => CopyChunksAsync(connection, publisher, context, cts.Token)) };

            for (var i = 1; i < Math.Min(workers, chunks.Count); i++)
            {
                tasks.Add(RunAsync(RunOtherWorkerAsync));
            }

            try
            {
                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
            catch when (!token.IsCancellationRequested)
            {
                // the error of the worker that failed, not the cancellation of the others
                var error = tasks.Where(t => t.IsFaulted)
                                 .SelectMany(t => t.Exception!.InnerExceptions)
                                 .FirstOrDefault(e => e is not OperationCanceledException);

                if (error != null) ExceptionDispatchInfo.Capture(error).Throw();

                throw;
            }

            return progress.Rows;
        }

        private static async Task<NpgsqlConnection> ImportSnapshotAsync(NpgsqlDataSource dataSource, PublishedTable table, string snapshotName, CancellationToken token)
        {
            var connection = dataSource.CreateConnection();

            try
            {
                await connection.OpenAsync(token).ConfigureAwait(false);

                using var cmd = connection.CreateCommand();

                cmd.CommandText = "SET DATESTYLE TO ISO";
                await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);

                // the snapshot of the sync slot: the copy sees exactly the transactions the slot will not send
                cmd.CommandText = $"BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET TRANSACTION SNAPSHOT '{snapshotName.Replace("'", "''")}'";
                await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);

                return connection;
            }
            catch (Exception ex)
            {
                await connection.DisposeAsync().ConfigureAwait(false);

                if (token.IsCancellationRequested) throw;

                throw new SnapshotUnavailableException($"Could not import the snapshot of the copy of {table.TableName}", ex);
            }
        }

        /// <summary>
        /// Copies chunks until there are none left. A chunk is copied once the publisher has confirmed all of it.
        /// </summary>
        private static async Task CopyChunksAsync(NpgsqlConnection connection, IMessagePublisher publisher, CopyContext context, CancellationToken token)
        {
            var jsonMessage = new JsonMessage { IsTableSync = true };

            jsonMessage.TableName.Append(context.Table.TableName);

            // every publisher gets the schema with its first row
            var schemaWritten = false;

            var value = new StringBuilder(256);
            var values = new List<string>(100);

            while (context.Chunks.TryDequeue(out var chunk))
            {
                var query = $"{context.Select}{chunk.RelationName} WHERE ctid >= '({chunk.Page},0)'"
                    + (chunk.IsLast ? "" : $" AND ctid < '({chunk.Page + PagesPerChunk},0)'")
                    + context.RowFilter;

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

                        GetValues(values, value, line, context.Logger);

                        if (values.Count != context.Columns.Count)
                        {
                            throw new Exception("Inconsistent column count between COPY output and table metadata");
                        }

                        jsonMessage.Json.Append("{\"c\":\"C\"");
                        jsonMessage.Json.Append(",\"w\":0");
                        jsonMessage.Json.Append(",\"n\":0");

                        if (context.JsonOptions.WriteTableNames)
                        {
                            jsonMessage.Json.Append(",\"t\":\"");
                            JsonUtils.EscapeText(jsonMessage.Json, context.Table.TableName);
                            jsonMessage.Json.Append('"');
                        }

                        if (!schemaWritten)
                        {
                            schemaWritten = true;

                            jsonMessage.Json.Append(',');
                            switch (context.JsonOptions.WriteMode)
                            {
                                case JsonWriteMode.Compact:
                                    WriteSchemaCompact(jsonMessage.Json, context.Table, context.Columns, context.IncludedColumns);
                                    break;
                                default:
                                    WriteSchemaDefault(jsonMessage.Json, context.Table, context.Columns, context.IncludedColumns);
                                    break;
                            }
                        }

                        WriteRow(context.Columns, values, jsonMessage.Json, jsonMessage.KeyKolValue, jsonMessage.PartitionKolValue,
                                 context.JsonOptions, context.IncludedColumns, context.PartitionKeyColumns);

                        jsonMessage.Json.Append('}');

                        await publisher.PublishAsync(jsonMessage, token).ConfigureAwait(false);

                        MetricsHelper.IncrementPublishCounter();

                        chunkRows++;

                        if (++unconfirmed >= context.ListenerOptions.BatchSize)
                        {
                            await publisher.ConfirmAsync(token).ConfigureAwait(false);
                            unconfirmed = 0;
                        }
                    }
                }

                await publisher.ConfirmAsync(token).ConfigureAwait(false);

                context.Progress.SetCopied(chunk.Relation, chunk.Page, chunkRows);
            }
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
