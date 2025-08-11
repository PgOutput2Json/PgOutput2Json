using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;
using NpgsqlTypes;

namespace PgOutput2Json
{
    public class JsonMessage
    {
        public ulong WalSeqNo { get; internal set; }
        public int Partition { get; internal set; }
        public StringBuilder Json { get; internal set; } = new(256);
        public StringBuilder TableName { get; internal set; } = new(256); 
        public StringBuilder KeyKolValue { get; internal set; } = new(256);
    }

    public class JsonWriter
    {
        private StringBuilder JsonBuilder => _result.Json;
        private StringBuilder TableNameBuilder => _result.TableName;
        private StringBuilder KeyColValueBuilder => _result.KeyKolValue;

        private readonly JsonOptions _jsonOptions;
        private readonly ReplicationListenerOptions _listenerOptions;

        private readonly JsonMessage _result = new();

        public JsonWriter(JsonOptions jsonOptions, ReplicationListenerOptions listenerOptions)
        {
            _jsonOptions = jsonOptions;
            _listenerOptions = listenerOptions;
        }

        public async Task<JsonMessage> WriteMessageAsync(ReplicationMessage replMessage, NpgsqlLogSequenceNumber virtualLsn, CancellationToken token)
        {
            var partition = -1;

            var message = replMessage.Message;
            var commitTimeStamp = replMessage.CommitTimeStamp;
            var hasRelationChanged = replMessage.HasRelationChanged;

            if (message is InsertMessage insertMsg)
            {
                partition = await WriteTupleAsync(insertMsg,
                                                  insertMsg.Relation,
                                                  insertMsg.NewRow,
                                                  null,
                                                  "I",
                                                  commitTimeStamp,
                                                  hasRelationChanged,
                                                  virtualLsn,
                                                  token)
                    .ConfigureAwait(false);
            }
            else if (message is DefaultUpdateMessage updateMsg)
            {
                partition = await WriteTupleAsync(updateMsg,
                                                  updateMsg.Relation,
                                                  updateMsg.NewRow,
                                                  null,
                                                  "U",
                                                  commitTimeStamp,
                                                  hasRelationChanged,
                                                  virtualLsn,
                                                  token)
                    .ConfigureAwait(false);
            }
            else if (message is FullUpdateMessage fullUpdateMsg)
            {
                partition = await WriteTupleAsync(fullUpdateMsg,
                                                  fullUpdateMsg.Relation,
                                                  fullUpdateMsg.NewRow,
                                                  fullUpdateMsg.OldRow,
                                                  "U",
                                                  commitTimeStamp,
                                                  hasRelationChanged,
                                                  virtualLsn,
                                                  token)
                    .ConfigureAwait(false);
            }
            else if (message is IndexUpdateMessage indexUpdateMsg)
            {
                partition = await WriteTupleAsync(indexUpdateMsg,
                                                  indexUpdateMsg.Relation,
                                                  indexUpdateMsg.NewRow,
                                                  indexUpdateMsg.Key,
                                                  "U",
                                                  commitTimeStamp,
                                                  hasRelationChanged,
                                                  virtualLsn,
                                                  token)
                    .ConfigureAwait(false);
            }
            else if (message is KeyDeleteMessage keyDeleteMsg)
            {
                partition = await WriteTupleAsync(keyDeleteMsg,
                                                  keyDeleteMsg.Relation,
                                                  null,
                                                  keyDeleteMsg.Key,
                                                  "D",
                                                  commitTimeStamp,
                                                  hasRelationChanged,
                                                  virtualLsn,
                                                  token)
                    .ConfigureAwait(false);
            }
            else if (message is FullDeleteMessage fullDeleteMsg)
            {
                partition = await WriteTupleAsync(fullDeleteMsg,
                                                  fullDeleteMsg.Relation,
                                                  null,
                                                  fullDeleteMsg.OldRow,
                                                  "D",
                                                  commitTimeStamp,
                                                  hasRelationChanged,
                                                  virtualLsn,
                                                  token)
                    .ConfigureAwait(false);
            }

            _result.Partition = partition;
            _result.WalSeqNo = message != null ? (ulong)message.WalEnd : 0;

            return _result;
        }

        private async Task<int> WriteTupleAsync(TransactionalMessage msg,
                                                RelationMessage relation,
                                                ReplicationTuple? newRow,
                                                ReplicationTuple? keyRow,
                                                string changeType,
                                                DateTime commitTimeStamp,
                                                bool hasRelationChanged,
                                                NpgsqlLogSequenceNumber virtualLsn,
                                                CancellationToken cancellationToken)
        {
            TableNameBuilder.Clear();
            TableNameBuilder.Append(relation.Namespace);
            TableNameBuilder.Append('.');
            TableNameBuilder.Append(relation.RelationName);
            
            var tableName = TableNameBuilder.ToString();

            JsonBuilder.Clear();
            JsonBuilder.Append("{\"c\":\"");
            JsonBuilder.Append(changeType);
            JsonBuilder.Append('"');

            JsonBuilder.Append(",\"w\":");
            JsonBuilder.Append((ulong)virtualLsn);

            if (_jsonOptions.WriteTimestamps)
            {
                JsonBuilder.Append(",\"cts\":");
                JsonBuilder.Append(commitTimeStamp.Ticks);
                JsonBuilder.Append(",\"mts\":");
                JsonBuilder.Append(msg.ServerClock.Ticks);
            }

            if (_jsonOptions.WriteTableNames)
            {
                JsonBuilder.Append(",\"t\":\"");
                JsonUtils.EscapeText(JsonBuilder, relation.Namespace);
                JsonBuilder.Append('.');
                JsonUtils.EscapeText(JsonBuilder, relation.RelationName);
                JsonBuilder.Append('"');
            }

            KeyColValueBuilder.Clear();

            if (!_listenerOptions.TablePartitions.TryGetValue(tableName, out var partitionCount))
            {
                partitionCount = 1;
            }

            if (!_listenerOptions.IncludedColumns.TryGetValue(tableName, out var includedCols))
            {
                includedCols = null;
            }

            if (hasRelationChanged)
            {
                JsonBuilder.Append(',');
                if (_jsonOptions.WriteMode == JsonWriteMode.Compact)
                {
                    WriteSchemaCompact(relation, includedCols);
                }
                else
                {
                    WriteSchema(relation, includedCols);
                }
            }

            int? hash = null;

            if (keyRow != null)
            {
                JsonBuilder.Append(",\"k\":");
                JsonBuilder.Append(_jsonOptions.WriteMode == JsonWriteMode.Compact ? '[' : '{');

                var writeToKeyBuilder = newRow == null; // only write key values if new row is not present (deletes)
                
                hash = await WriteValuesAsync(keyRow, relation, writeToKeyBuilder, includedCols, true, cancellationToken)
                        .ConfigureAwait(false);

                JsonBuilder.Append(_jsonOptions.WriteMode == JsonWriteMode.Compact ? ']' : '}');
            }

            if (newRow != null)
            {
                JsonBuilder.Append(",\"r\":");
                JsonBuilder.Append(_jsonOptions.WriteMode == JsonWriteMode.Compact ? '[' : '{');

                hash = await WriteValuesAsync(newRow, relation, true, includedCols, false, cancellationToken)
                    .ConfigureAwait(false);

                JsonBuilder.Append(_jsonOptions.WriteMode == JsonWriteMode.Compact ? ']' : '}');
            }

            JsonBuilder.Append('}');

            return hash.HasValue ? hash.Value % partitionCount : 0;
        }

        private async Task<int> WriteValuesAsync(ReplicationTuple tuple,
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

                if (!IsIncluded(includedCols, col) 
                    || (isKeyRow && !isKeyColumn) 
                    || (!value.IsDBNull && !value.IsUnchangedToastedValue && value.Kind != TupleDataKind.TextValue))
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
                    keyColBuilder = KeyColValueBuilder;

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
                    JsonBuilder.Append(',');
                }

                firstValue = false;

                if (_jsonOptions.WriteMode != JsonWriteMode.Compact)
                {
                    // skip property name writing if we are in compact mode

                    JsonBuilder.Append('"');
                    JsonUtils.EscapeText(JsonBuilder, col.ColumnName);
                    JsonBuilder.Append('"');
                    JsonBuilder.Append(':');
                }

                if (value.IsUnchangedToastedValue)
                {
                    JsonBuilder.Append("\"__TOAST__\"");
                    keyColBuilder?.Append("\"__TOAST__\"");
                }
                else if (value.IsDBNull)
                {
                    JsonBuilder.Append("null");
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
                        hash = JsonUtils.WriteNumber(JsonBuilder, colValue);
                        if (keyColBuilder != null) JsonUtils.WriteNumber(keyColBuilder, colValue);
                    }
                    else if (pgOid.IsBoolean())
                    {
                        hash = JsonUtils.WriteBoolean(JsonBuilder, colValue);
                        if (keyColBuilder != null) JsonUtils.WriteBoolean(keyColBuilder, colValue);
                    }
                    else if (pgOid.IsByte())
                    {
                        hash = JsonUtils.WriteByte(JsonBuilder, colValue);
                        if (keyColBuilder != null) JsonUtils.WriteByte(keyColBuilder, colValue);
                    }
                    else if (pgOid.IsArrayOfNumber())
                    {
                        hash = JsonUtils.WriteArrayOfNumber(JsonBuilder, colValue);
                        if (keyColBuilder != null) JsonUtils.WriteArrayOfNumber(keyColBuilder, colValue);
                    }
                    else if (pgOid.IsArrayOfByte())
                    {
                        hash = JsonUtils.WriteArrayOfByte(JsonBuilder, colValue);
                        if (keyColBuilder != null) JsonUtils.WriteArrayOfByte(keyColBuilder, colValue);
                    }
                    else if (pgOid.IsArrayOfBoolean())
                    {
                        hash = JsonUtils.WriteArrayOfBoolean(JsonBuilder, colValue);
                        if (keyColBuilder != null) JsonUtils.WriteArrayOfBoolean(keyColBuilder, colValue);
                    }
                    else if (pgOid.IsArrayOfText())
                    {
                        hash = JsonUtils.WriteArrayOfText(JsonBuilder, colValue);
                        if (keyColBuilder != null) JsonUtils.WriteArrayOfText(keyColBuilder, colValue);
                    }
                    else
                    {
                        hash = JsonUtils.WriteText(JsonBuilder, colValue);
                        if (keyColBuilder != null) JsonUtils.WriteText(keyColBuilder, colValue);
                    }

                    if (isKeyColumn) finalHash ^= hash;
                }
            }

            if (KeyColValueBuilder.Length > 0 )
            {
                KeyColValueBuilder.Append(']');
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
            JsonBuilder.Append("\"schema\":{");

            JsonBuilder.Append("\"tableName\":");
            JsonBuilder.Append('"');
            JsonUtils.EscapeText(JsonBuilder, relation.Namespace);
            JsonBuilder.Append('.');
            JsonUtils.EscapeText(JsonBuilder, relation.RelationName);
            JsonBuilder.Append('"');

            JsonBuilder.Append(",\"columns\":[");

            var i = 0;
            foreach (var col in relation.Columns)
            {
                if (!IsIncluded(includedCols, col)) continue;

                var isKeyColumn = (col.Flags & RelationMessage.Column.ColumnFlags.PartOfKey)
                    == RelationMessage.Column.ColumnFlags.PartOfKey;

                if (i > 0) JsonBuilder.Append(',');

                JsonBuilder.Append('{');

                JsonBuilder.Append("\"name\":");
                JsonBuilder.Append('"');
                JsonUtils.EscapeText(JsonBuilder, col.ColumnName);
                JsonBuilder.Append('"');

                JsonBuilder.Append(",\"isKey\":");
                JsonBuilder.Append(isKeyColumn ? "true" : "false");

                JsonBuilder.Append(",\"dataType\":");
                JsonUtils.EscapeText(JsonBuilder, col.DataTypeId.ToString());

                if (col.TypeModifier != -1)
                {
                    JsonBuilder.Append(",\"typeModifier\":");
                    JsonUtils.EscapeText(JsonBuilder, col.TypeModifier.ToString());
                }

                JsonBuilder.Append('}');
                i++;
            }

            JsonBuilder.Append(']'); // columns
            JsonBuilder.Append('}'); // schema
        }

        private void WriteSchemaCompact(RelationMessage relation, IReadOnlyList<string>? includedCols)
        {
            JsonBuilder.Append("\"s\":[");
            JsonBuilder.Append('"');
            JsonUtils.EscapeText(JsonBuilder, relation.Namespace);
            JsonBuilder.Append('.');
            JsonUtils.EscapeText(JsonBuilder, relation.RelationName);
            JsonBuilder.Append('"');

            foreach (var col in relation.Columns)
            {
                if (!IsIncluded(includedCols, col)) continue;

                JsonBuilder.Append(',');
                JsonBuilder.Append('[');

                var isKeyColumn = (col.Flags & RelationMessage.Column.ColumnFlags.PartOfKey)
                    == RelationMessage.Column.ColumnFlags.PartOfKey;

                JsonBuilder.Append('"');
                JsonUtils.EscapeText(JsonBuilder, col.ColumnName);
                JsonBuilder.Append('"');
                JsonBuilder.Append(',');

                JsonBuilder.Append(isKeyColumn ? '1' : '0');
                JsonBuilder.Append(',');

                JsonUtils.EscapeText(JsonBuilder, col.DataTypeId.ToString());

                if (col.TypeModifier != -1)
                {
                    JsonBuilder.Append(',');
                    JsonUtils.EscapeText(JsonBuilder, col.TypeModifier.ToString());
                }

                JsonBuilder.Append(']');
            }

            JsonBuilder.Append(']');
        }
    }
}
