using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;

namespace PgOutput2Json
{
    class MessageWriterOld: IMessageWriter
    {
        private readonly StringBuilder _jsonBuilder = new StringBuilder(256);
        private readonly StringBuilder _tableNameBuilder = new StringBuilder(256);
        private readonly StringBuilder _keyColValueBuilder = new StringBuilder(256);

        private readonly JsonOptions _jsonOptions;
        private readonly ReplicationListenerOptions _listenerOptions;

        private readonly MessageWriterResult _result = new MessageWriterResult();

        public MessageWriterOld(JsonOptions jsonOptions, ReplicationListenerOptions listenerOptions)
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
                                             "I",
                                             commitTimeStamp,
                                             _jsonOptions.WriteNulls,
                                             token)
                    .ConfigureAwait(false);
            }
            else if (message is UpdateMessage updateMsg)
            {
                partition = await WriteTuple(updateMsg,
                                             updateMsg.Relation,
                                             updateMsg.NewRow,
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
                                           ReplicationTuple tuple,
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
            _jsonBuilder.Append("{\"_ct\":\"");
            _jsonBuilder.Append(changeType);
            _jsonBuilder.Append('"');

            if (_jsonOptions.WriteWalStart)
            {
                _jsonBuilder.Append(',');
                _jsonBuilder.Append("\"_ws\":\"");
                _jsonBuilder.Append((ulong)msg.WalStart);
            }

            _jsonBuilder.Append(',');
            _jsonBuilder.Append("\"_we\":");
            _jsonBuilder.Append((ulong)msg.WalEnd);

            if (_jsonOptions.WriteTimestamps)
            {
                _jsonBuilder.Append(',');
                _jsonBuilder.Append("\"_cts\":\"");
                _jsonBuilder.Append(commitTimeStamp.Ticks);
                _jsonBuilder.Append("\",");
                _jsonBuilder.Append("\"_mts\":\"");
                _jsonBuilder.Append(msg.ServerClock.Ticks);
                _jsonBuilder.Append('"');
            }

            if (_jsonOptions.WriteTableNames)
            {
                _jsonBuilder.Append(',');
                _jsonBuilder.Append("\"_tbl\":\"");
                JsonUtils.EscapeText(_jsonBuilder, relation.Namespace);
                _jsonBuilder.Append('.');
                JsonUtils.EscapeText(_jsonBuilder, relation.RelationName);
                _jsonBuilder.Append('"');
            }

            _keyColValueBuilder.Clear();

            int finalHash = 0x12345678;

            if (!_listenerOptions.TablePartitions.TryGetValue(tableName, out var partitionCount))
            {
                partitionCount = 1;
            }

            if (!_listenerOptions.IncludedColumns.TryGetValue(tableName, out var includedCols))
            {
                includedCols = null;
            }

            var i = 0;

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

                    StringBuilder? valueBuilder = null;

                    if (isKeyColumn)
                    {
                        valueBuilder = _keyColValueBuilder;
                        if (valueBuilder.Length > 0)
                        {
                            valueBuilder.Append('|');
                        }
                    }

                    _jsonBuilder.Append(",\"");
                    JsonUtils.EscapeText(_jsonBuilder, col.ColumnName);
                    _jsonBuilder.Append("\":");

                    if (value.IsDBNull)
                    {
                        _jsonBuilder.Append("null");
                    }
                    else if (value.Kind == TupleDataKind.TextValue)
                    {
                        var pgOid = (PgOid)col.DataTypeId;

                        var colValue = await value.Get<string>(cancellationToken)
                            .ConfigureAwait(false);

                        valueBuilder?.Append(colValue);

                        int hash;

                        if (pgOid.IsNumber())
                        {
                            hash = JsonUtils.WriteNumber(_jsonBuilder, colValue);
                        }
                        else if (pgOid.IsBoolean())
                        {
                            hash = JsonUtils.WriteBoolean(_jsonBuilder, colValue);
                        }
                        else if (pgOid.IsByte())
                        {
                            hash = JsonUtils.WriteByte(_jsonBuilder, colValue);
                        }
                        else if (pgOid.IsArrayOfNumber())
                        {
                            hash = JsonUtils.WriteArrayOfNumber(_jsonBuilder, colValue);
                        }
                        else if (pgOid.IsArrayOfByte())
                        {
                            hash = JsonUtils.WriteArrayOfByte(_jsonBuilder, colValue);
                        }
                        else if (pgOid.IsArrayOfBoolean())
                        {
                            hash = JsonUtils.WriteArrayOfBoolean(_jsonBuilder, colValue);
                        }
                        else if (pgOid.IsArrayOfText())
                        {
                            hash = JsonUtils.WriteArrayOfText(_jsonBuilder, colValue);
                        }
                        else 
                        {
                            hash = JsonUtils.WriteText(_jsonBuilder, colValue);
                        }

                        if (isKeyColumn) finalHash ^= hash;
                    }
                }
            }
                            
            _jsonBuilder.Append('}');

            return finalHash % partitionCount;
        }
    }
}
