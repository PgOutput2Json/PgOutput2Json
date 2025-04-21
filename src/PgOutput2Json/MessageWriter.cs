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
        Task<MessageWriterResult> WriteMessage(PgOutputReplicationMessage message, DateTime commitTimeStamp, CancellationToken cancellationToken);
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
                                             _jsonOptions.WriteNulls,
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
                                             _jsonOptions.WriteNulls,
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
                                             _jsonOptions.WriteNulls,
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
                                             _jsonOptions.WriteNulls,
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
                                             false,
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
                                             false,
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
                                           bool sendNulls,
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
                _jsonBuilder.Append(",\"ws\":\"");
                _jsonBuilder.Append((ulong)msg.WalStart);
            }

            _jsonBuilder.Append(",\"w\":");
            _jsonBuilder.Append((ulong)msg.WalEnd);

            if (_jsonOptions.WriteTimestamps)
            {
                _jsonBuilder.Append(",\"cts\":\"");
                _jsonBuilder.Append(commitTimeStamp.Ticks);
                _jsonBuilder.Append(",\"mts\":\"");
                _jsonBuilder.Append(msg.ServerClock.Ticks);
                _jsonBuilder.Append('"');
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

            int? hash = null;

            if (keyRow != null)
            {
                _jsonBuilder.Append(",\"k\":{");

                var writeKeyValues = newRow == null; // only write key values if new row is not present (deletes)

                hash = await WriteValues(keyRow, relation, sendNulls, writeKeyValues, null, cancellationToken)
                    .ConfigureAwait(false);

                _jsonBuilder.Append('}');
            }

            if (newRow != null)
            {
                _jsonBuilder.Append(",\"r\":{");

                hash = await WriteValues(newRow, relation, sendNulls, true, includedCols, cancellationToken)
                    .ConfigureAwait(false);

                _jsonBuilder.Append('}');
            }

            _jsonBuilder.Append('}');

            return hash.HasValue ? hash.Value % partitionCount : 0;
        }

        private async Task<int> WriteValues(ReplicationTuple tuple,
                                            RelationMessage relation,
                                            bool sendNulls,
                                            bool writeKeyValues,
                                            IReadOnlyList<string>? includedCols,
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

                if (value.IsDBNull && !sendNulls) continue;

                if (value.IsDBNull || value.Kind == TupleDataKind.TextValue)
                {
                    var isIncluded = includedCols == null; // if not specified, included by default

                    if (includedCols != null)
                    {
                        foreach (var c in includedCols)
                        {
                            if (c == col.ColumnName)
                            {
                                isIncluded = true;
                                break;
                            }
                        }
                    }

                    if (!isIncluded && !isKeyColumn)
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

                    if (isKeyColumn && writeKeyValues)
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

                    _jsonBuilder.Append('"');
                    JsonUtils.EscapeText(_jsonBuilder, col.ColumnName);
                    _jsonBuilder.Append('"');
                    _jsonBuilder.Append(':');

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
            }

            if (_keyColValueBuilder.Length > 0 )
            {
                _keyColValueBuilder.Append(']');
            }

            return finalHash;
        }
    }
}
