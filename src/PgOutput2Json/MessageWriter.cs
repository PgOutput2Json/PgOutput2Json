using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;

namespace PgOutput2Json
{
    interface IMessageWriter
    {
        Task<MessageWriterResult> WriteMessage(PgOutputReplicationMessage message, DateTime commitTimeStamp, bool hasRelationChanged, CancellationToken cancellationToken);
    }

    class MessageWriterResult
    {
        public int Partition;
        public string Json = "";
        public string TableNames = "";
        public string KeyKolValue = "";
    }

    class MessageWriter: IMessageWriter
    {
        private readonly StringBuilder _jsonBuilder = new StringBuilder(256);
        private readonly StringBuilder _tableNameBuilder = new StringBuilder(256);
        private readonly StringBuilder _keyColValueBuilder = new StringBuilder(256);

        private readonly JsonOptions _jsonOptions;
        private readonly ReplicationListenerOptions _listenerOptions;

        private readonly MessageWriterResult _result = new MessageWriterResult();

        public MessageWriter(JsonOptions jsonOptions, ReplicationListenerOptions listenerOptions)
        {
            _jsonOptions = jsonOptions;
            _listenerOptions = listenerOptions;
        }

        public async Task<MessageWriterResult> WriteMessage(PgOutputReplicationMessage message,
                                                            DateTime commitTimeStamp,
                                                            bool hasRelationChanged,
                                                            CancellationToken token)
        {
            var partition = -1;

            if (message is InsertMessage insertMsg)
            {
                partition = await WriteTuple(insertMsg,
                                             insertMsg.Relation,
                                             insertMsg.NewRow,
                                             null,
                                             "I",
                                             commitTimeStamp,
                                             hasRelationChanged,
                                             token)
                    .ConfigureAwait(false);
            }
            else if (message is DefaultUpdateMessage updateMsg)
            {
                partition = await WriteTuple(updateMsg,
                                             updateMsg.Relation,
                                             updateMsg.NewRow,
                                             null,
                                             "U",
                                             commitTimeStamp,
                                             hasRelationChanged,
                                             token)
                    .ConfigureAwait(false);
            }
            else if (message is FullUpdateMessage fullUpdateMsg)
            {
                partition = await WriteTuple(fullUpdateMsg,
                                             fullUpdateMsg.Relation,
                                             fullUpdateMsg.NewRow,
                                             fullUpdateMsg.OldRow,
                                             "U",
                                             commitTimeStamp,
                                             hasRelationChanged,
                                             token)
                    .ConfigureAwait(false);
            }
            else if (message is IndexUpdateMessage indexUpdateMsg)
            {
                partition = await WriteTuple(indexUpdateMsg,
                                             indexUpdateMsg.Relation,
                                             indexUpdateMsg.NewRow,
                                             indexUpdateMsg.Key,
                                             "U",
                                             commitTimeStamp,
                                             hasRelationChanged,
                                             token)
                    .ConfigureAwait(false);
            }
            else if (message is KeyDeleteMessage keyDeleteMsg)
            {
                partition = await WriteTuple(keyDeleteMsg,
                                             keyDeleteMsg.Relation,
                                             null,
                                             keyDeleteMsg.Key,
                                             "D",
                                             commitTimeStamp,
                                             hasRelationChanged,
                                             token)
                    .ConfigureAwait(false);
            }
            else if (message is FullDeleteMessage fullDeleteMsg)
            {
                partition = await WriteTuple(fullDeleteMsg,
                                             fullDeleteMsg.Relation,
                                             null,
                                             fullDeleteMsg.OldRow,
                                             "D",
                                             commitTimeStamp,
                                             hasRelationChanged,
                                             token)
                    .ConfigureAwait(false);
            }

            _result.Partition = partition;
            _result.Json = _jsonBuilder.ToString();
            _result.TableNames = _tableNameBuilder.ToString();
            _result.KeyKolValue = _keyColValueBuilder.ToString();

            return _result;
        }

        private async Task<int> WriteTuple(TransactionalMessage msg,
                                           RelationMessage relation,
                                           ReplicationTuple? newRow,
                                           ReplicationTuple? keyRow,
                                           string changeType,
                                           DateTime commitTimeStamp,
                                           bool hasRelationChanged,
                                           CancellationToken cancellationToken)
        {
            _tableNameBuilder.Clear();
            _tableNameBuilder.Append(relation.Namespace);
            _tableNameBuilder.Append('.');
            _tableNameBuilder.Append(relation.RelationName);
            
            var tableName = _tableNameBuilder.ToString();

            _jsonBuilder.Clear();
            _jsonBuilder.Append("{\"c\":\"");
            _jsonBuilder.Append(changeType);
            _jsonBuilder.Append('"');

            if (_jsonOptions.WriteWalStart)
            {
                _jsonBuilder.Append(",\"ws\":");
                _jsonBuilder.Append((ulong)msg.WalStart);
            }

            _jsonBuilder.Append(",\"w\":");
            _jsonBuilder.Append((ulong)msg.WalEnd);

            if (_jsonOptions.WriteTimestamps)
            {
                _jsonBuilder.Append(",\"cts\":");
                _jsonBuilder.Append(commitTimeStamp.Ticks);
                _jsonBuilder.Append(",\"mts\":");
                _jsonBuilder.Append(msg.ServerClock.Ticks);
            }

            if (_jsonOptions.WriteTableNames)
            {
                _jsonBuilder.Append(",\"t\":\"");
                JsonUtils.EscapeText(_jsonBuilder, relation.Namespace);
                _jsonBuilder.Append('.');
                JsonUtils.EscapeText(_jsonBuilder, relation.RelationName);
                _jsonBuilder.Append('"');
            }

            _keyColValueBuilder.Clear();

            if (!_listenerOptions.TablePartitions.TryGetValue(tableName, out var partitionCount))
            {
                partitionCount = 1;
            }

            if (!_listenerOptions.IncludedColumns.TryGetValue(tableName, out var includedCols))
            {
                includedCols = null;
            }

            if (_jsonOptions.WriteMode == JsonWriteMode.Compact && hasRelationChanged)
            {
                _jsonBuilder.Append(',');
                WriteSchema(relation, includedCols);
            }

            int? hash = null;

            if (keyRow != null)
            {
                _jsonBuilder.Append(",\"k\":");
                _jsonBuilder.Append(_jsonOptions.WriteMode == JsonWriteMode.Compact ? '[' : '{');

                var writeToKeyBuilder = newRow == null; // only write key values if new row is not present (deletes)
                
                hash = await WriteValues(keyRow, relation, writeToKeyBuilder, includedCols, true, cancellationToken)
                        .ConfigureAwait(false);

                _jsonBuilder.Append(_jsonOptions.WriteMode == JsonWriteMode.Compact ? ']' : '}');
            }

            if (newRow != null)
            {
                _jsonBuilder.Append(",\"r\":");
                _jsonBuilder.Append(_jsonOptions.WriteMode == JsonWriteMode.Compact ? '[' : '{');

                hash = await WriteValues(newRow, relation, true, includedCols, false, cancellationToken)
                    .ConfigureAwait(false);

                _jsonBuilder.Append(_jsonOptions.WriteMode == JsonWriteMode.Compact ? ']' : '}');
            }

            _jsonBuilder.Append('}');

            return hash.HasValue ? hash.Value % partitionCount : 0;
        }

        private async Task<int> WriteValues(ReplicationTuple tuple,
                                            RelationMessage relation,
                                            bool writeToKeyValueBuilder,
                                            IReadOnlyList<string>? includedCols,
                                            bool isKeyRow,
                                            CancellationToken cancellationToken)
        {
            int finalHash = 0x12345678;

            var i = 0;
            var firstValue = true;

            await foreach (var value in tuple.ConfigureAwait(false))
            {
                var col = relation.Columns[i++];

                var isKeyColumn = (col.Flags & RelationMessage.Column.ColumnFlags.PartOfKey)
                    == RelationMessage.Column.ColumnFlags.PartOfKey;

                // we must write nulls in compact mode to preserve the column indexes (no column names)
                if (value.IsDBNull && !_jsonOptions.WriteNulls && _jsonOptions.WriteMode != JsonWriteMode.Compact)
                {
                    continue;
                }

                if (!IsIncluded(includedCols, col) || (isKeyRow && !isKeyColumn) || (!value.IsDBNull && value.Kind != TupleDataKind.TextValue))
                {
                    if (!value.IsDBNull)
                    {
                        // this is a hack to skip bytes (dispose does the trick)
                        // otherwise, npgsql throws exception
                        await value.Get<string>(cancellationToken)
                            .ConfigureAwait(false);
                    }
                    continue;
                }

                StringBuilder? keyColBuilder = null;

                if (isKeyColumn && writeToKeyValueBuilder)
                {
                    keyColBuilder = _keyColValueBuilder;

                    if (keyColBuilder.Length == 0)
                    {
                        keyColBuilder.Append('[');
                    }
                    else
                    {
                        keyColBuilder.Append(',');
                    }
                }

                if (!firstValue)
                {
                    _jsonBuilder.Append(',');
                }

                firstValue = false;

                if (_jsonOptions.WriteMode != JsonWriteMode.Compact)
                {
                    // skip property name writing if we are in compact mode

                    _jsonBuilder.Append('"');
                    JsonUtils.EscapeText(_jsonBuilder, col.ColumnName);
                    _jsonBuilder.Append('"');
                    _jsonBuilder.Append(':');
                }

                if (value.IsDBNull)
                {
                    _jsonBuilder.Append("null");
                    keyColBuilder?.Append("null");
                }
                else if (value.Kind == TupleDataKind.TextValue)
                {
                    var pgOid = (PgOid)col.DataTypeId;

                    var colValue = await value.Get<string>(cancellationToken)
                        .ConfigureAwait(false);

                    int hash;

                    if (pgOid.IsNumber())
                    {
                        hash = JsonUtils.WriteNumber(_jsonBuilder, colValue);
                        if (keyColBuilder != null) JsonUtils.WriteNumber(keyColBuilder, colValue);
                    }
                    else if (pgOid.IsBoolean())
                    {
                        hash = JsonUtils.WriteBoolean(_jsonBuilder, colValue);
                        if (keyColBuilder != null) JsonUtils.WriteBoolean(keyColBuilder, colValue);
                    }
                    else if (pgOid.IsByte())
                    {
                        hash = JsonUtils.WriteByte(_jsonBuilder, colValue);
                        if (keyColBuilder != null) JsonUtils.WriteByte(keyColBuilder, colValue);
                    }
                    else if (pgOid.IsArrayOfNumber())
                    {
                        hash = JsonUtils.WriteArrayOfNumber(_jsonBuilder, colValue);
                        if (keyColBuilder != null) JsonUtils.WriteArrayOfNumber(keyColBuilder, colValue);
                    }
                    else if (pgOid.IsArrayOfByte())
                    {
                        hash = JsonUtils.WriteArrayOfByte(_jsonBuilder, colValue);
                        if (keyColBuilder != null) JsonUtils.WriteArrayOfByte(keyColBuilder, colValue);
                    }
                    else if (pgOid.IsArrayOfBoolean())
                    {
                        hash = JsonUtils.WriteArrayOfBoolean(_jsonBuilder, colValue);
                        if (keyColBuilder != null) JsonUtils.WriteArrayOfBoolean(keyColBuilder, colValue);
                    }
                    else if (pgOid.IsArrayOfText())
                    {
                        hash = JsonUtils.WriteArrayOfText(_jsonBuilder, colValue);
                        if (keyColBuilder != null) JsonUtils.WriteArrayOfText(keyColBuilder, colValue);
                    }
                    else
                    {
                        hash = JsonUtils.WriteText(_jsonBuilder, colValue);
                        if (keyColBuilder != null) JsonUtils.WriteText(keyColBuilder, colValue);
                    }

                    if (isKeyColumn) finalHash ^= hash;
                }
            }

            if (_keyColValueBuilder.Length > 0 )
            {
                _keyColValueBuilder.Append(']');
            }

            return finalHash;
        }

        private static bool IsIncluded(IReadOnlyList<string>? includedCols, RelationMessage.Column col)
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

        private void WriteSchema(RelationMessage relation, IReadOnlyList<string>? includedCols)
        {
            _jsonBuilder.Append("\"s\":[");
            _jsonBuilder.Append('"');
            JsonUtils.EscapeText(_jsonBuilder, relation.Namespace);
            _jsonBuilder.Append('.');
            JsonUtils.EscapeText(_jsonBuilder, relation.RelationName);
            _jsonBuilder.Append('"');

            foreach (var col in relation.Columns)
            {
                if (!IsIncluded(includedCols, col)) continue;

                _jsonBuilder.Append(',');
                _jsonBuilder.Append('[');

                var isKeyColumn = (col.Flags & RelationMessage.Column.ColumnFlags.PartOfKey)
                    == RelationMessage.Column.ColumnFlags.PartOfKey;

                _jsonBuilder.Append('"');
                JsonUtils.EscapeText(_jsonBuilder, col.ColumnName);
                _jsonBuilder.Append('"');
                _jsonBuilder.Append(',');

                _jsonBuilder.Append(isKeyColumn ? '1' : '0');
                _jsonBuilder.Append(',');

                JsonUtils.EscapeText(_jsonBuilder, col.DataTypeId.ToString());

                if (col.TypeModifier != -1)
                {
                    _jsonBuilder.Append(',');
                    JsonUtils.EscapeText(_jsonBuilder, col.TypeModifier.ToString());
                }

                _jsonBuilder.Append(']');
            }

            _jsonBuilder.Append(']');
        }
    }
}
