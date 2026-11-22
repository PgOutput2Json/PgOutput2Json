using System.Collections.Generic;
using System.Text;

using Npgsql.Replication.PgOutput.Messages;

namespace PgOutput2Json
{
    /// <summary>
    /// What is kept of a relation of a replication connection, from its Relation message until the next one: its joined
    /// name (<see cref="PgName"/>), and for an <see cref="IRowPublisher"/> its <see cref="RowTable"/> and the index of each
    /// column of the relation in it (-1 for a column left out). Only these are kept: the Relation message is reused by Npgsql.
    /// </summary>
    internal sealed class RelationEntry
    {
        public required string Name { get; init; }
        public RowTable? Table { get; set; }
        public int[]? Indexes { get; set; }
    }

    /// <summary>
    /// The relations of a replication connection, by relation id: one per listener connection and one per sync worker. The
    /// entry of a relation is made when its Relation message arrives, so a row only looks it up.
    /// </summary>
    internal sealed class Relations
    {
        private readonly Dictionary<uint, RelationEntry> _entries = [];

        /// <summary>On a Relation message: a new entry, with the table of the row reader if the publisher takes rows.</summary>
        public RelationEntry Set(RelationMessage relation, RowReader? rowReader)
        {
            var builder = new StringBuilder(relation.Namespace.Length + relation.RelationName.Length + 1);

            PgName.Append(builder, relation.Namespace, relation.RelationName);

            var entry = new RelationEntry { Name = builder.ToString() };

            if (rowReader != null) (entry.Table, entry.Indexes) = rowReader.CreateTable(relation, entry.Name);

            return _entries[relation.RelationId] = entry;
        }

        /// <summary>The entry of the relation of a row; PostgreSQL sends the Relation message first, so it is there.</summary>
        public RelationEntry Get(RelationMessage relation, RowReader? rowReader)
        {
            return _entries.TryGetValue(relation.RelationId, out var entry) ? entry : Set(relation, rowReader);
        }
    }
}
