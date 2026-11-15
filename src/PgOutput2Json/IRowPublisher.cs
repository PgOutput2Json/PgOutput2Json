using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace PgOutput2Json
{
    /// <summary>
    /// A publisher that takes the changed rows themselves, instead of their JSON. The listener, the table sync
    /// and the initial copy call <see cref="PublishRowAsync"/> for every insert, update, delete and copied row,
    /// so a store does not parse the JSON that the library would otherwise write for it.
    /// Logical decoding messages (change type "M") have no row and still go to <see cref="IMessagePublisher.PublishAsync"/>.
    /// Batching, <see cref="IMessagePublisher.ConfirmAsync"/> and the positions work as for the JSON.
    /// </summary>
    public interface IRowPublisher : IMessagePublisher
    {
        /// <summary>
        /// Publishes one row. The row and its value lists are reused for the next row, so they are valid only until
        /// the returned task completes: keep what is needed, not the objects. The <see cref="ChangeRow.Table"/> can be kept.
        /// </summary>
        Task PublishRowAsync(ChangeRow row, CancellationToken token);
    }

    public enum RowChangeType
    {
        Insert,
        Update,
        Delete,

        /// <summary>A row of the initial data copy, with no position (<see cref="ChangeRow.TxFinalLsn"/> and <see cref="ChangeRow.MessageNo"/> are 0).</summary>
        Copy,
    }

    public enum RowValueKind
    {
        Null,
        Text,

        /// <summary>A TOASTed value that the update did not change: PostgreSQL does not send it, and the stored value stays.</summary>
        Unchanged,
    }

    /// <summary>
    /// A value as PostgreSQL's text representation, the same in the stream and in the copy.
    /// </summary>
    public readonly struct RowValue
    {
        public RowValueKind Kind { get; }

        /// <summary>The text, or null unless <see cref="Kind"/> is <see cref="RowValueKind.Text"/>.</summary>
        public string? Text { get; }

        private RowValue(RowValueKind kind, string? text)
        {
            Kind = kind;
            Text = text;
        }

        public static RowValue Null => default;
        public static RowValue Unchanged => new(RowValueKind.Unchanged, null);
        public static RowValue FromText(string text) => new(RowValueKind.Text, text);

        public override string ToString() => Kind == RowValueKind.Text ? Text! : Kind.ToString();
    }

    public sealed class RowColumn
    {
        public required string Name { get; init; }

        /// <summary>The oid of the type, see <c>pg_type</c>.</summary>
        public uint TypeId { get; init; }

        /// <summary>The type modifier (<c>atttypmod</c>), -1 if there is none.</summary>
        public int TypeModifier { get; init; }

        /// <summary>
        /// The name of a type that is not built in (for a domain the name of its base type), as the JSON schema has it;
        /// null for a built-in type.
        /// </summary>
        public string? TypeName { get; init; }

        /// <summary>Part of the replica identity: the primary key by default.</summary>
        public bool IsKey { get; init; }

        /// <summary>One of the partition key columns of the table (<see cref="PgOutput2JsonBuilder.WithPartitionKeyFields"/>).</summary>
        public bool IsPartitionKey { get; init; }
    }

    /// <summary>
    /// A published table and its columns, without the ones left out with <see cref="PgOutput2JsonBuilder.WithPgColumns"/>.
    /// The rows of a table share one instance until its columns may have changed (a Relation message); then they get a new one.
    /// </summary>
    public sealed class RowTable
    {
        /// <summary>schema.table</summary>
        public string Name { get; }

        public IReadOnlyList<RowColumn> Columns { get; }

        public RowTable(string name, IReadOnlyList<RowColumn> columns)
        {
            Name = name;
            Columns = columns;
        }
    }

    public sealed class ChangeRow
    {
        internal readonly List<RowValue> NewValueList = new(64);
        internal readonly List<RowValue> OldValueList = new(64);

        public RowChangeType ChangeType { get; internal set; }

        public RowTable Table { get; internal set; } = null!;

        /// <summary>
        /// The row after the change, one value per column of <see cref="Table"/>; null for a delete.
        /// </summary>
        public IReadOnlyList<RowValue>? NewValues { get; internal set; }

        /// <summary>
        /// The row before the change, for a delete and for an update that changed the key or of a table with
        /// REPLICA IDENTITY FULL; otherwise null. One value per column of <see cref="Table"/>: the key columns,
        /// and null for the others, or the whole old row with REPLICA IDENTITY FULL.
        /// </summary>
        public IReadOnlyList<RowValue>? OldValues { get; internal set; }

        /// <summary>The deduplication key, "w" in the JSON: the final LSN of the transaction.</summary>
        public ulong TxFinalLsn { get; internal set; }

        /// <summary>The deduplication key, "n" in the JSON: the number of the change in its transaction.</summary>
        public ulong MessageNo { get; internal set; }

        /// <summary>
        /// True for the rows of a table's initial sync, see <see cref="JsonMessage.IsTableSync"/>: a publisher that saves its position must not save theirs.
        /// </summary>
        public bool IsTableSync { get; internal set; }

        /// <summary>The commit time of the transaction ("cts" in the JSON); default for a copied row.</summary>
        public DateTime CommitTimestamp { get; internal set; }

        /// <summary>The server's clock when the change was sent ("mts" in the JSON); default for a copied row.</summary>
        public DateTime ServerClock { get; internal set; }
    }
}
