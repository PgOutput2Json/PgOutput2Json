using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;
using System.Text;

namespace PgOutput2Json.Core
{
    public class ReplicationListener
    {
        private readonly ReplicationListenerOptions _options;
        private readonly StringBuilder _jsonBuilder = new StringBuilder(256);
        private readonly StringBuilder _tableNameBuilder = new StringBuilder(256);

        public ReplicationListener(ReplicationListenerOptions options)
        {
            _options = options;
        }

        public async Task ListenForChanges(CancellationToken cancellationToken)
        {
            while (true)
            {
                try
                {
                    await using var conn = new LogicalReplicationConnection("server=localhost;database=repl_test_db;username=replicator;password=replicator");
                    await conn.Open();

                    _options.LoggingInfoHandler?.Invoke("Connected to PostgreSQL");

                    var slot = new PgOutputReplicationSlot("test_slot");
                    var replOptions = new PgOutputReplicationOptions("pub_test", 1);

                    DateTime commitTimeStamp = DateTime.UtcNow;

                    await foreach (var message in conn.StartReplication(slot, replOptions, cancellationToken))
                    {
                        if (message is BeginMessage beginMsg)
                        {
                            commitTimeStamp = beginMsg.TransactionCommitTimestamp;
                        }
                        else if (message is InsertMessage insertMsg)
                        {
                            await WriteTuple(insertMsg.NewRow,
                                             insertMsg.Relation,
                                             "I",
                                             commitTimeStamp,
                                             insertMsg.ServerClock,
                                             _options.WriteNulls);
                        }
                        else if (message is UpdateMessage updateMsg)
                        {
                            await WriteTuple(updateMsg.NewRow,
                                             updateMsg.Relation,
                                             "U",
                                             commitTimeStamp,
                                             updateMsg.ServerClock,
                                             _options.WriteNulls);
                        }
                        else if (message is KeyDeleteMessage keyDeleteMsg)
                        {
                            await WriteTuple(keyDeleteMsg.Key,
                                             keyDeleteMsg.Relation,
                                             "D",
                                             commitTimeStamp,
                                             keyDeleteMsg.ServerClock,
                                             false);
                        }
                        else if (message is FullDeleteMessage fullDeleteMsg)
                        {
                            await WriteTuple(fullDeleteMsg.OldRow,
                                             fullDeleteMsg.Relation,
                                             "D",
                                             commitTimeStamp,
                                             fullDeleteMsg.ServerClock,
                                             false);
                        }

                        // Always call SetReplicationStatus() or assign LastAppliedLsn and LastFlushedLsn individually
                        // so that Npgsql can inform the server which WAL files can be removed/recycled.

                        conn.SetReplicationStatus(message.WalEnd);
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    if (ex.Message.StartsWith("55006:"))
                    {
                        _options.LoggingWarnHandler?.Invoke("Slot taken - waiting for 10 seconds...");
                    }
                    else
                    {
                        _options.LoggingErrorHandler?.Invoke(ex, "Error in replication listener. Waiting for 10 seconds...");
                    }

                    Thread.Sleep(10000);
                }
            }
        }

        private async Task WriteTuple(ReplicationTuple tuple,
                                             RelationMessage relation,
                                             string changeType,
                                             DateTime commitTimeStamp,
                                             DateTime messageTimeStamp,
                                             bool sendNulls)
        {
            _tableNameBuilder.Clear();
            _tableNameBuilder.Append(relation.Namespace);
            _tableNameBuilder.Append('.');
            _tableNameBuilder.Append(relation.RelationName);

            _jsonBuilder.Clear();
            _jsonBuilder.Append("{\"_ct\":\"");
            _jsonBuilder.Append(changeType);
            _jsonBuilder.Append("\",");
            _jsonBuilder.Append("\"_cts\":\"");
            _jsonBuilder.Append(commitTimeStamp.Ticks);
            _jsonBuilder.Append("\",");
            _jsonBuilder.Append("\"_mts\":\"");
            _jsonBuilder.Append(messageTimeStamp.Ticks);
            _jsonBuilder.Append("\",");
            _jsonBuilder.Append("\"_re\":\"");
            _jsonBuilder.Append(relation.Namespace);
            _jsonBuilder.Append('.');
            _jsonBuilder.Append(relation.RelationName);
            _jsonBuilder.Append('"');

            var i = 0;

            if (!_options.Partitions.TryGetValue(_tableNameBuilder.ToString(), out var partionConfig))
            {
                partionConfig = null;
            }

            int partition = 0;

            await foreach (var value in tuple)
            {
                var col = relation.Columns[i++];

                if (value.IsDBNull && !sendNulls) continue;

                if (value.IsDBNull || value.Kind == TupleDataKind.TextValue)
                {
                    var isPartitionCol = partionConfig != null && col.ColumnName == partionConfig.ColumnName;

                    _jsonBuilder.Append(",\"");
                    _jsonBuilder.Append(col.ColumnName);
                    _jsonBuilder.Append("\":");

                    if (value.IsDBNull)
                    {
                        _jsonBuilder.Append("null");
                    }
                    else if (value.Kind == TupleDataKind.TextValue)
                    {
                        var type = value.GetPostgresType();
                        var pgOid = (PgOid)type.OID;

                        int hash;

                        if (pgOid.IsNumber())
                        {
                            hash = JsonUtils.WriteNumber(_jsonBuilder, value.GetTextReader());
                        }
                        else if (pgOid.IsBoolean())
                        {
                            hash = JsonUtils.WriteBoolean(_jsonBuilder, value.GetTextReader());
                        }
                        else if (pgOid.IsByte())
                        {
                            hash = JsonUtils.WriteByte(_jsonBuilder, value.GetTextReader());
                        }
                        else
                        {
                            hash = JsonUtils.WriteText(_jsonBuilder, value.GetTextReader());
                        }

                        if (isPartitionCol)
                        {
                            partition = hash % partionConfig!.PartitionCount;
                        }
                    }
                }
            }

            _jsonBuilder.Append('}');

            _options.MessageHandler?.Invoke(_jsonBuilder, _tableNameBuilder, partition);
        }
    }
}
