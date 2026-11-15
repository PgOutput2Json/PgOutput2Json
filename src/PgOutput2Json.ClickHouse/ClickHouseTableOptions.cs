using System.Collections.Generic;

namespace PgOutput2Json.ClickHouse
{
    public enum ClickHouseTableEngine
    {
        /// <summary>
        /// Keeps the latest version of every row; read the table with FINAL. The default.
        /// </summary>
        ReplacingMergeTree,

        /// <summary>
        /// Keeps every version of every row, deletes included: the history of the table, ordered by _version.
        /// A change that PostgreSQL replays after a reconnect is there twice, with the same _version.
        /// </summary>
        MergeTree,

        /// <summary>
        /// Keeps nothing: only the materialized views on the table get the rows.
        /// </summary>
        Null,
    }

    /// <summary>
    /// How the output creates the ClickHouse table of a PostgreSQL table. It is used only when the table is created:
    /// ClickHouse cannot change the ORDER BY, the PARTITION BY or the engine of an existing table.
    /// </summary>
    public sealed class ClickHouseTableOptions
    {
        /// <summary>
        /// The ORDER BY: the key columns, in the order you want. ClickHouse replaces and deletes the rows by it, so it
        /// must have every key column and no other. To sort by more columns, make them part of the replica identity
        /// (REPLICA IDENTITY USING INDEX on a unique index). Default: the key columns, in the order of the table.
        /// A table without a key gets only inserts, and can be ordered by any of its columns.
        /// </summary>
        public IReadOnlyList<string>? OrderBy { get; set; }

        /// <summary>
        /// The PARTITION BY expression, e.g. "intDiv(id, 1000000)". ClickHouse replaces rows only within a partition,
        /// and the row of a delete has only the key columns, so the expression may use only key columns. A table
        /// without a key may use any of its columns. Default: no partitions.
        /// </summary>
        public string? PartitionBy { get; set; }

        public ClickHouseTableEngine Engine { get; set; } = ClickHouseTableEngine.ReplacingMergeTree;
    }
}
