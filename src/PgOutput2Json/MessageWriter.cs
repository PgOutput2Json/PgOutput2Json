using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;

namespace PgOutput2Json
{
    class MessageWriter
    {
        public class Result
        {
            public int Partition;
            public string Json = "";
            public string TableNames = "";
            public string KeyKolValue = "";
        }

        private readonly StringBuilder _jsonBuilder = new StringBuilder(256);
        private readonly StringBuilder _tableNameBuilder = new StringBuilder(256);
        private readonly StringBuilder _keyColValueBuilder = new StringBuilder(256);

        private readonly JsonOptions _jsonOptions;
        private readonly ReplicationListenerOptions _listenerOptions;

        private readonly Result _result = new Result();

        public MessageWriter(JsonOptions jsonOptions, ReplicationListenerOptions listenerOptions)
        {
            _jsonOptions = jsonOptions;
            _listenerOptions = listenerOptions;
        }

        public async Task<Result> WriteMessage(PgOutputReplicationMessage message,
                                               DateTime commitTimeStamp,
                                               CancellationToken token)
        {
            var partition = -1;

            if (message is BeginMessage beginMsg)
            {
                commitTimeStamp = beginMsg.TransactionCommitTimestamp;
            }
            else if (message is InsertMessage insertMsg)
            {
                partition = await WriteTuple(insertMsg.NewRow,
                                 insertMsg.Relation,
                                 "I",
                                 commitTimeStamp,
                                 insertMsg.ServerClock,
                                 _jsonOptions.WriteNulls,
                                 token)
                    .ConfigureAwait(false);
            }
            else if (message is UpdateMessage updateMsg)
            {
                partition = await WriteTuple(updateMsg.NewRow,
                                 updateMsg.Relation,
                                 "U",
                                 commitTimeStamp,
                                 updateMsg.ServerClock,
                                 _jsonOptions.WriteNulls,
                                 token)
                    .ConfigureAwait(false);
            }
            else if (message is KeyDeleteMessage keyDeleteMsg)
            {
                partition = await WriteTuple(keyDeleteMsg.Key,
                                 keyDeleteMsg.Relation,
                                 "D",
                                 commitTimeStamp,
                                 keyDeleteMsg.ServerClock,
                                 false,
                                 token)
                    .ConfigureAwait(false);
            }
            else if (message is FullDeleteMessage fullDeleteMsg)
            {
                partition = await WriteTuple(fullDeleteMsg.OldRow,
                                 fullDeleteMsg.Relation,
                                 "D",
                                 commitTimeStamp,
                                 fullDeleteMsg.ServerClock,
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

        private async Task<int> WriteTuple(ReplicationTuple tuple,
                                           RelationMessage relation,
                                           string changeType,
                                           DateTime commitTimeStamp,
                                           DateTime messageTimeStamp,
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

            if (_jsonOptions.WriteTimestamps)
            {
                _jsonBuilder.Append(",");
                _jsonBuilder.Append("\"_cts\":\"");
                _jsonBuilder.Append(commitTimeStamp.Ticks);
                _jsonBuilder.Append("\",");
                _jsonBuilder.Append("\"_mts\":\"");
                _jsonBuilder.Append(messageTimeStamp.Ticks);
                _jsonBuilder.Append('"');
            }

            if (_jsonOptions.WriteTableNames)
            {
                _jsonBuilder.Append(",");
                _jsonBuilder.Append("\"_tbl\":\"");
                JsonUtils.EscapeText(_jsonBuilder, relation.Namespace);
                _jsonBuilder.Append('.');
                JsonUtils.EscapeText(_jsonBuilder, relation.RelationName);
                _jsonBuilder.Append('"');
            }

            _keyColValueBuilder.Clear();

            int finalHash = 0x12345678;

            if (!_listenerOptions.KeyColumns.TryGetValue(tableName, out var keyColumn))
            {
                keyColumn = null;
            }

            if (!_listenerOptions.IncludedColumns.TryGetValue(tableName, out var includedCols))
            {
                includedCols = null;
            }

            var i = 0;

            await foreach (var value in tuple.ConfigureAwait(false))
            {
                var col = relation.Columns[i++];

                if (value.IsDBNull && !sendNulls) continue;

                if (value.IsDBNull || value.Kind == TupleDataKind.TextValue)
                {
                    var isKeyColumn = false;
                    if (keyColumn != null)
                    {
                        foreach (var c in keyColumn.ColumnNames)
                        {
                            if (c == col.ColumnName)
                            {
                                isKeyColumn = true;
                                break;
                            }
                        }
                    }

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

            var partition = keyColumn != null ? finalHash % keyColumn.PartitionCount : 0;

            if (_listenerOptions.PartitionFilter != null 
                && (partition < _listenerOptions.PartitionFilter.FromInclusive || partition >= _listenerOptions.PartitionFilter.ToExclusive))
            {
                partition = -1; // this will prevent event from firing
            }

            return partition;
        }
    }
}
