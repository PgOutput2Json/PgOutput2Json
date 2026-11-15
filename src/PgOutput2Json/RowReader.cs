using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;

namespace PgOutput2Json
{
    /// <summary>
    /// Reads the changes of the stream into a <see cref="ChangeRow"/>, for an <see cref="IRowPublisher"/>:
    /// what <see cref="JsonWriter"/> does for the JSON. One per stream, like the writer.
    /// </summary>
    internal sealed class RowReader
    {
        private readonly ReplicationListenerOptions _options;

        private readonly ChangeRow _row = new();

        // the names of the types that are not built in, from the Type messages that pgoutput sends before a Relation message
        private readonly Dictionary<uint, string> _typeNames = [];

        // by relation id: the table, and the index of each column of the relation in it (-1 for a column left out)
        private readonly Dictionary<uint, (RowTable Table, int[] Indexes)> _tables = [];

        public RowReader(ReplicationListenerOptions options)
        {
            _options = options;
        }

        public void AddType(TypeMessage type)
        {
            _typeNames[type.TypeId] = type.Name;
        }

        /// <summary>
        /// The row of an insert, update or delete. <see cref="ReplicationMessage.HasRelationChanged"/> makes a new <see cref="RowTable"/>.
        /// </summary>
        public async Task<ChangeRow> ReadAsync(ReplicationMessage message, CancellationToken token)
        {
            var (changeType, relation, newRow, oldRow) = message.Message switch
            {
                InsertMessage m => (RowChangeType.Insert, m.Relation, m.NewRow, (ReplicationTuple?)null),
                DefaultUpdateMessage m => (RowChangeType.Update, m.Relation, m.NewRow, null),
                FullUpdateMessage m => (RowChangeType.Update, m.Relation, m.NewRow, m.OldRow),
                IndexUpdateMessage m => (RowChangeType.Update, m.Relation, m.NewRow, m.Key),
                KeyDeleteMessage m => (RowChangeType.Delete, m.Relation, (ReplicationTuple?)null, m.Key),
                FullDeleteMessage m => (RowChangeType.Delete, m.Relation, null, m.OldRow),
                _ => throw new InvalidOperationException($"Not a row: {message.Message?.GetType().Name}"),
            };

            if (message.HasRelationChanged || !_tables.TryGetValue(relation.RelationId, out var table))
            {
                _tables[relation.RelationId] = table = CreateTable(relation);
            }

            var row = _row;

            row.ChangeType = changeType;
            row.Table = table.Table;
            row.TxFinalLsn = message.TransactionFinalLsn;
            row.MessageNo = message.MessageNo;
            row.IsTableSync = false;
            row.CommitTimestamp = message.CommitTimeStamp;
            row.ServerClock = ((TransactionalMessage)message.Message).ServerClock;

            // the old row comes first in the message
            row.OldValues = oldRow == null ? null : await ReadValuesAsync(oldRow, table.Indexes, row.OldValueList, token).ConfigureAwait(false);
            row.NewValues = newRow == null ? null : await ReadValuesAsync(newRow, table.Indexes, row.NewValueList, token).ConfigureAwait(false);

            return row;
        }

        private static async Task<List<RowValue>> ReadValuesAsync(ReplicationTuple tuple, int[] indexes, List<RowValue> values, CancellationToken token)
        {
            values.Clear();

            var i = 0;

            await foreach (var value in tuple.ConfigureAwait(false))
            {
                var index = indexes[i++];

                if (value.IsDBNull)
                {
                    if (index >= 0) values.Add(RowValue.Null);
                }
                else if (value.IsUnchangedToastedValue)
                {
                    if (index >= 0) values.Add(RowValue.Unchanged);
                }
                else if (value.Kind == TupleDataKind.TextValue)
                {
                    // read also when left out: Npgsql needs the value read before the next one
                    var text = await value.Get<string>(token).ConfigureAwait(false);

                    if (index >= 0) values.Add(RowValue.FromText(text));
                }
                else
                {
                    // the listener asks for text values only
                    throw new InvalidOperationException($"Unexpected value kind {value.Kind}");
                }
            }

            return values;
        }

        private (RowTable, int[]) CreateTable(RelationMessage relation)
        {
            var name = $"{relation.Namespace}.{relation.RelationName}";

            _options.IncludedColumns.TryGetValue(name, out var includedColumns);
            _options.PartitionKeyColumns.TryGetValue(name, out var partitionKeyColumns);

            var columns = new List<RowColumn>(relation.Columns.Count);
            var indexes = new int[relation.Columns.Count];

            for (var i = 0; i < relation.Columns.Count; i++)
            {
                var col = relation.Columns[i];

                if (includedColumns != null && !Contains(includedColumns, col.ColumnName))
                {
                    indexes[i] = -1;
                    continue;
                }

                indexes[i] = columns.Count;

                columns.Add(new RowColumn
                {
                    Name = col.ColumnName,
                    TypeId = col.DataTypeId,
                    TypeModifier = col.TypeModifier,
                    TypeName = _typeNames.TryGetValue(col.DataTypeId, out var typeName) ? typeName : null,
                    IsKey = (col.Flags & RelationMessage.Column.ColumnFlags.PartOfKey) != 0,
                    IsPartitionKey = partitionKeyColumns != null && Contains(partitionKeyColumns, col.ColumnName),
                });
            }

            return (new RowTable(name, columns), indexes);
        }

        internal static bool Contains(IReadOnlyList<string> names, string name)
        {
            foreach (var n in names)
            {
                if (n == name) return true;
            }

            return false;
        }
    }
}
