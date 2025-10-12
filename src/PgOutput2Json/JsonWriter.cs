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
        public StringBuilder Json { get; internal set; } = new(256);
        public StringBuilder TableName { get; internal set; } = new(256); 
        public StringBuilder KeyKolValue { get; internal set; } = new(256);
        public StringBuilder PartitionKolValue { get; internal set; } = new(256);
    }

    public class JsonWriter
    {
        private StringBuilder JsonBuilder => _result.Json;
        private StringBuilder TableNameBuilder => _result.TableName;
        private StringBuilder KeyColValueBuilder => _result.KeyKolValue;
        private StringBuilder PartitionColValueBuilder => _result.PartitionKolValue;

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
            var message = replMessage.Message;
            var commitTimeStamp = replMessage.CommitTimeStamp;
            var hasRelationChanged = replMessage.HasRelationChanged;

            if (message is InsertMessage insertMsg)
            {
                await WriteTupleAsync(insertMsg,
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
                await WriteTupleAsync(updateMsg,
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
                await WriteTupleAsync(fullUpdateMsg,
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
                await WriteTupleAsync(indexUpdateMsg,
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
                await WriteTupleAsync(keyDeleteMsg,
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
                await WriteTupleAsync(fullDeleteMsg,
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

            _result.WalSeqNo = message != null ? (ulong)message.WalEnd : 0;

            return _result;
        }

        private async Task WriteTupleAsync(TransactionalMessage msg,
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
                if (_jsonOptions.TimestampFormat == TimestampFormat.UnixTimeMilliseconds)
                {
                    JsonBuilder.Append(",\"cts\":");
                    JsonBuilder.Append(new DateTimeOffset(commitTimeStamp).ToUnixTimeMilliseconds());
                    JsonBuilder.Append(",\"mts\":");
                    JsonBuilder.Append(new DateTimeOffset(msg.ServerClock).ToUnixTimeMilliseconds());
                }
                else
                {
                    JsonBuilder.Append(",\"cts\":");
                    JsonBuilder.Append(commitTimeStamp.Ticks);
                    JsonBuilder.Append(",\"mts\":");
                    JsonBuilder.Append(msg.ServerClock.Ticks);
                }
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
            PartitionColValueBuilder.Clear();

            if (!_listenerOptions.IncludedColumns.TryGetValue(tableName, out var includedCols))
            {
                includedCols = null;
            }

            if (!_listenerOptions.PartitionKeyColumns.TryGetValue(tableName, out var partitionKeyFields))
            {
                partitionKeyFields = null;
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

            if (keyRow != null)
            {
                JsonBuilder.Append(",\"k\":");
                JsonBuilder.Append(_jsonOptions.WriteMode == JsonWriteMode.Compact ? '[' : '{');

                var writeToKeyBuilder = newRow == null; // only write key values if new row is not present (deletes)
                
                await WriteValuesAsync(keyRow, relation, writeToKeyBuilder, includedCols, partitionKeyFields, true, cancellationToken)
                    .ConfigureAwait(false);

                JsonBuilder.Append(_jsonOptions.WriteMode == JsonWriteMode.Compact ? ']' : '}');
            }

            if (newRow != null)
            {
                JsonBuilder.Append(",\"r\":");
                JsonBuilder.Append(_jsonOptions.WriteMode == JsonWriteMode.Compact ? '[' : '{');

                await WriteValuesAsync(newRow, relation, true, includedCols, partitionKeyFields, false, cancellationToken)
                    .ConfigureAwait(false);

                JsonBuilder.Append(_jsonOptions.WriteMode == JsonWriteMode.Compact ? ']' : '}');
            }

            JsonBuilder.Append('}');
        }

        private async Task WriteValuesAsync(ReplicationTuple tuple,
                                            RelationMessage relation,
                                            bool writeToKeyValueBuilder,
                                            IReadOnlyList<string>? includedCols,
                                            IReadOnlyList<string>? partitionKeyFields,
                                            bool isKeyRow,
                                            CancellationToken cancellationToken)
        {
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
                    keyColBuilder.Append(keyColBuilder.Length == 0 ? '[' : ',');
                }

                StringBuilder? partitionKeyBuilder = null;

                if (IsPartitionKeyCol(partitionKeyFields, col))
                {
                    partitionKeyBuilder = PartitionColValueBuilder;
                    partitionKeyBuilder.Append(partitionKeyBuilder.Length == 0 ? '[' : ',');
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

                    if (pgOid.IsNumber())
                    {
                        JsonUtils.WriteNumber(JsonBuilder, colValue);
                        if (keyColBuilder != null) JsonUtils.WriteNumber(keyColBuilder, colValue);
                        if (partitionKeyBuilder != null) JsonUtils.WriteNumber(partitionKeyBuilder, colValue);
                    }
                    else if (pgOid.IsBoolean())
                    {
                        JsonUtils.WriteBoolean(JsonBuilder, colValue);
                        if (keyColBuilder != null) JsonUtils.WriteBoolean(keyColBuilder, colValue);
                        if (partitionKeyBuilder != null) JsonUtils.WriteBoolean(partitionKeyBuilder, colValue);
                    }
                    else if (pgOid.IsByte())
                    {
                        JsonUtils.WriteByte(JsonBuilder, colValue);
                        if (keyColBuilder != null) JsonUtils.WriteByte(keyColBuilder, colValue);
                        if (partitionKeyBuilder != null) JsonUtils.WriteByte(partitionKeyBuilder, colValue);
                    }
                    else if (pgOid.IsArrayOfNumber())
                    {
                        JsonUtils.WriteArrayOfNumber(JsonBuilder, colValue);
                        if (keyColBuilder != null) JsonUtils.WriteArrayOfNumber(keyColBuilder, colValue);
                        if (partitionKeyBuilder != null) JsonUtils.WriteArrayOfNumber(partitionKeyBuilder, colValue);
                    }
                    else if (pgOid.IsArrayOfByte())
                    {
                        JsonUtils.WriteArrayOfByte(JsonBuilder, colValue);
                        if (keyColBuilder != null) JsonUtils.WriteArrayOfByte(keyColBuilder, colValue);
                        if (partitionKeyBuilder != null) JsonUtils.WriteArrayOfByte(partitionKeyBuilder, colValue);
                    }
                    else if (pgOid.IsArrayOfBoolean())
                    {
                        JsonUtils.WriteArrayOfBoolean(JsonBuilder, colValue);
                        if (keyColBuilder != null) JsonUtils.WriteArrayOfBoolean(keyColBuilder, colValue);
                        if (partitionKeyBuilder != null) JsonUtils.WriteArrayOfBoolean(partitionKeyBuilder, colValue);
                    }
                    else if (pgOid.IsArrayOfText())
                    {
                        JsonUtils.WriteArrayOfText(JsonBuilder, colValue);
                        if (keyColBuilder != null) JsonUtils.WriteArrayOfText(keyColBuilder, colValue);
                        if (partitionKeyBuilder != null) JsonUtils.WriteArrayOfText(partitionKeyBuilder, colValue);
                    }
                    else
                    {
                        JsonUtils.WriteText(JsonBuilder, colValue);
                        if (keyColBuilder != null) JsonUtils.WriteText(keyColBuilder, colValue);
                        if (partitionKeyBuilder != null) JsonUtils.WriteText(partitionKeyBuilder, colValue);
                    }
                }
            }

            if (KeyColValueBuilder.Length > 0 )
            {
                KeyColValueBuilder.Append(']');
            }

            if (PartitionColValueBuilder.Length > 0)
            {
                PartitionColValueBuilder.Append("]");
            }
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

        private static bool IsPartitionKeyCol(IReadOnlyList<string>? partitionKeyFields, RelationMessage.Column col)
        {
            if (partitionKeyFields == null) return false;

            foreach (var c in partitionKeyFields)
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
