using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace PgOutput2Json
{
    /// <summary>
    /// A table of the configured publications. The row filters of all the publications that contain
    /// the table are combined: a change is published if any of them accepts it.
    /// </summary>
    internal sealed record PublishedTable(string TableName, string QuotedTableName, uint Oid, string? RowFilter, IReadOnlyList<string>? Columns = null);

    /// <summary>
    /// The stored state of a published table. For "copied" the LSN is the consistent point of the copy,
    /// for "ready" it is the position from which the main stream owns the table (0 if it always did).
    /// </summary>
    internal sealed record TableState(PublishedTable Table, string State, ulong Lsn);

    /// <summary>
    /// Keeps the state of every published table, per replication slot, in the source database:
    /// "init" (needs the initial copy), "copying" (sync slot created, copy running), "copied" (copy done,
    /// the sync slot holds the position) or "ready" (owned by the main stream). The state is written even
    /// when the initial copy is off, so that enabling it later copies only the tables added since.
    /// A temporary slot has no identity, so nothing is stored for it.
    /// </summary>
    internal static class TableSyncState
    {
        public const string Schema = "pgoutput2json";

        public const string StateInit = "init";
        public const string StateCopying = "copying";
        public const string StateCopied = "copied";
        public const string StateReady = "ready";

        private const string FinalMarker = "__FINAL__"; // "all tables copied" row of the old progress table

        /// <summary>
        /// Brings the stored states in line with the publications and returns the state of every published table.
        /// With the initial copy off, every table is returned as ready, whatever is stored.
        /// </summary>
        public static async Task<List<TableState>> RegisterTablesAsync(ReplicationListenerOptions options, ILogger? logger, CancellationToken token)
        {
            using var cn = new NpgsqlConnection(options.ConnectionString);

            await cn.OpenAsync(token).ConfigureAwait(false);

            var tables = await GetPublishedTablesAsync(cn, options, null, token).ConfigureAwait(false);

            if (!IsTracked(options))
            {
                return tables.Select(t => new TableState(t, options.CopyData ? StateInit : StateReady, 0)).ToList();
            }

            try
            {
                return await RegisterTablesAsync(cn, tables, options, logger, token).ConfigureAwait(false);
            }
            catch (PostgresException ex) when (!options.CopyData)
            {
                // streaming does not depend on the state, so a database that cannot store it (missing
                // privileges, a read-only standby) must not stop a listener that does not copy
                logger?.LogWarning(ex, "Could not store the table states in schema {Schema}. Streaming continues, " +
                    "but if the initial data copy is enabled later, all the tables will be copied.", Schema);

                return tables.Select(t => new TableState(t, StateReady, 0)).ToList();
            }
        }

        /// <summary>
        /// A table of the publications by its oid, or null if the publications do not contain it.
        /// </summary>
        public static async Task<PublishedTable?> FindPublishedTableAsync(ReplicationListenerOptions options, uint oid, CancellationToken token)
        {
            using var cn = new NpgsqlConnection(options.ConnectionString);

            await cn.OpenAsync(token).ConfigureAwait(false);

            return (await GetPublishedTablesAsync(cn, options, oid, token).ConfigureAwait(false)).FirstOrDefault();
        }

        public static async Task SetStateAsync(ReplicationListenerOptions options, PublishedTable table, string state, ulong lsn, CancellationToken token)
        {
            if (!IsTracked(options)) return;

            using var cn = new NpgsqlConnection(options.ConnectionString);

            await cn.OpenAsync(token).ConfigureAwait(false);

            await SetStateAsync(cn, null, options.ReplicationSlotName, table, state, lsn, token).ConfigureAwait(false);
        }

        private static bool IsTracked(ReplicationListenerOptions options)
        {
            return !options.UseTemporarySlot && !string.IsNullOrWhiteSpace(options.ReplicationSlotName);
        }

        private static async Task<List<TableState>> RegisterTablesAsync(NpgsqlConnection cn,
                                                                         List<PublishedTable> tables,
                                                                         ReplicationListenerOptions options,
                                                                         ILogger? logger,
                                                                         CancellationToken token)
        {
            var slotName = options.ReplicationSlotName;

            await EnsureTableAsync(cn, token).ConfigureAwait(false);

            using var tx = await cn.BeginTransactionAsync(token).ConfigureAwait(false);

            var states = await LoadStatesAsync(cn, tx, slotName, token).ConfigureAwait(false);

            // the first run for this slot - take over what the old progress table knows
            var copied = (All: false, Tables: new HashSet<string>());

            if (states.Count == 0)
            {
                copied = await LoadCopiedFromOldProgressAsync(cn, tx, slotName, token).ConfigureAwait(false);
            }

            var result = new List<TableState>();

            foreach (var table in tables)
            {
                // a table dropped and created again is a new table
                if (states.Remove(table.TableName, out var existing) && existing.Oid == table.Oid)
                {
                    if (existing.State == StateReady || options.CopyData)
                    {
                        result.Add(new TableState(table, existing.State, existing.Lsn));
                        continue;
                    }

                    // the copy was turned off before this table was synced: it is streamed without a copy,
                    // and stays "init" so it is copied if the copy is turned on again
                    if (existing.State != StateInit)
                    {
                        await SetStateAsync(cn, tx, slotName, table, StateInit, 0, token).ConfigureAwait(false);
                    }

                    result.Add(new TableState(table, StateReady, 0));
                    continue;
                }

                var isReady = !options.CopyData || copied.All || copied.Tables.Contains(table.TableName);
                var state = isReady ? StateReady : StateInit;

                await SetStateAsync(cn, tx, slotName, table, state, 0, token).ConfigureAwait(false);

                result.Add(new TableState(table, state, 0));

                logger?.LogInformation("Registered table {TableName} for slot {SlotName} as {State}", table.TableName, slotName, state);
            }

            // what is left is no longer published - forget it, so the table is copied if it is published again
            foreach (var tableName in states.Keys)
            {
                using var cmd = new NpgsqlCommand($"DELETE FROM {Schema}.table_sync_state WHERE slot_name = @slot_name AND table_name = @table_name", cn, tx);

                cmd.Parameters.AddWithValue("slot_name", slotName);
                cmd.Parameters.AddWithValue("table_name", tableName);

                await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);

                logger?.LogInformation("Table {TableName} is no longer published, removed its state for slot {SlotName}", tableName, slotName);
            }

            await tx.CommitAsync(token).ConfigureAwait(false);

            return result;
        }

        private static async Task EnsureTableAsync(NpgsqlConnection cn, CancellationToken token)
        {
            using var cmd = cn.CreateCommand();

            // CREATE SCHEMA needs the CREATE privilege on the database even if the schema exists,
            // so it is skipped when an administrator has already created the schema
            cmd.CommandText = $"SELECT to_regnamespace('{Schema}') IS NOT NULL";

            if (!(bool)(await cmd.ExecuteScalarAsync(token).ConfigureAwait(false))!)
            {
                cmd.CommandText = $"CREATE SCHEMA IF NOT EXISTS {Schema}";
                await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
            }

            cmd.CommandText = $@"
CREATE TABLE IF NOT EXISTS {Schema}.table_sync_state (
    slot_name text NOT NULL,
    table_name text NOT NULL,
    table_oid oid NOT NULL,
    state text NOT NULL,
    lsn pg_lsn,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT pk_table_sync_state PRIMARY KEY (slot_name, table_name)
)";
            await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }

        private static async Task<Dictionary<string, (uint Oid, string State, ulong Lsn)>> LoadStatesAsync(NpgsqlConnection cn, NpgsqlTransaction tx, string slotName, CancellationToken token)
        {
            var result = new Dictionary<string, (uint Oid, string State, ulong Lsn)>();

            using var cmd = new NpgsqlCommand($"SELECT table_name, table_oid, state, lsn FROM {Schema}.table_sync_state WHERE slot_name = @slot_name", cn, tx);

            cmd.Parameters.AddWithValue("slot_name", slotName);

            using var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);

            while (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                var lsn = await reader.IsDBNullAsync(3, token).ConfigureAwait(false) ? 0 : (ulong)(NpgsqlLogSequenceNumber)reader.GetValue(3);

                result[reader.GetString(0)] = ((uint)reader.GetValue(1), reader.GetString(2), lsn);
            }

            return result;
        }

        private static async Task<(bool All, HashSet<string> Tables)> LoadCopiedFromOldProgressAsync(NpgsqlConnection cn, NpgsqlTransaction tx, string slotName, CancellationToken token)
        {
            var tables = new HashSet<string>();

            using var cmd = new NpgsqlCommand($"SELECT to_regclass('{Schema}.data_copy_progress') IS NOT NULL", cn, tx);

            if (!(bool)(await cmd.ExecuteScalarAsync(token).ConfigureAwait(false))!) return (false, tables);

            cmd.CommandText = $"SELECT table_name FROM {Schema}.data_copy_progress WHERE slot_name = @slot_name AND is_completed";
            cmd.Parameters.AddWithValue("slot_name", slotName);

            using var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);

            while (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                tables.Add(reader.GetString(0));
            }

            return (tables.Contains(FinalMarker), tables);
        }

        private static async Task SetStateAsync(NpgsqlConnection cn, NpgsqlTransaction? tx, string slotName, PublishedTable table, string state, ulong lsn, CancellationToken token)
        {
            using var cmd = new NpgsqlCommand($@"
INSERT INTO {Schema}.table_sync_state (slot_name, table_name, table_oid, state, lsn)
VALUES (@slot_name, @table_name, @table_oid, @state, @lsn)
ON CONFLICT (slot_name, table_name)
DO UPDATE SET table_oid = EXCLUDED.table_oid, state = EXCLUDED.state, lsn = EXCLUDED.lsn, updated_at = now()", cn, tx);

            cmd.Parameters.AddWithValue("slot_name", slotName);
            cmd.Parameters.AddWithValue("table_name", table.TableName);
            cmd.Parameters.Add(new NpgsqlParameter("table_oid", NpgsqlDbType.Oid) { Value = table.Oid });
            cmd.Parameters.AddWithValue("state", state);
            cmd.Parameters.Add(new NpgsqlParameter("lsn", NpgsqlDbType.PgLsn) { Value = lsn == 0 ? DBNull.Value : new NpgsqlLogSequenceNumber(lsn) });

            await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }

        private static async Task<List<PublishedTable>> GetPublishedTablesAsync(NpgsqlConnection cn, ReplicationListenerOptions options, uint? oid, CancellationToken token)
        {
            using var cmd = cn.CreateCommand();

            // row filters and column lists exist since PostgreSQL 15
            var hasFilters = cn.PostgreSqlVersion.Major >= 15;

            cmd.CommandText = $@"
SELECT * FROM (
    SELECT schemaname, tablename, format('%I.%I', schemaname, tablename)::regclass::oid AS oid,
           {(hasFilters ? "rowfilter, attnames::text[]" : "NULL::text, NULL::text[]")}
    FROM pg_catalog.pg_publication_tables
    WHERE pubname = ANY(@publications) AND schemaname <> '{Schema}'
) t
WHERE @oid = 0 OR oid = @oid
ORDER BY schemaname, tablename";

            cmd.Parameters.AddWithValue("publications", options.PublicationNames);
            cmd.Parameters.Add(new NpgsqlParameter("oid", NpgsqlDbType.Oid) { Value = oid ?? 0 });

            var rows = new List<(string Schema, string Table, uint Oid, string? RowFilter, string[]? Columns)>();

            using (var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false))
            {
                while (await reader.ReadAsync(token).ConfigureAwait(false))
                {
                    rows.Add((reader.GetString(0),
                              reader.GetString(1),
                              (uint)reader.GetValue(2),
                              await reader.IsDBNullAsync(3, token).ConfigureAwait(false) ? null : reader.GetString(3),
                              await reader.IsDBNullAsync(4, token).ConfigureAwait(false) ? null : (string[])reader.GetValue(4)));
                }
            }

            // PostgreSQL does not allow different column lists for one table in the publications of
            // one stream, so the first list is the list
            return rows
                .GroupBy(r => (r.Schema, r.Table, r.Oid))
                .Select(g => new PublishedTable(
                    $"{g.Key.Schema}.{g.Key.Table}",
                    $"{Quote(g.Key.Schema)}.{Quote(g.Key.Table)}",
                    g.Key.Oid,
                    CombineRowFilters(g.Select(r => r.RowFilter).ToList()),
                    g.First().Columns))
                .ToList();
        }

        public static string Quote(string identifier) => $"\"{identifier.Replace("\"", "\"\"")}\"";

        /// <summary>
        /// A table in several publications is published if any of them accepts the row,
        /// and a publication without a filter accepts every row.
        /// </summary>
        internal static string? CombineRowFilters(IReadOnlyList<string?> rowFilters)
        {
            if (rowFilters.Any(string.IsNullOrWhiteSpace)) return null;

            var distinct = rowFilters.Distinct().ToList();

            return distinct.Count == 1 ? distinct[0] : string.Join(" OR ", distinct.Select(f => $"({f})"));
        }
    }
}
