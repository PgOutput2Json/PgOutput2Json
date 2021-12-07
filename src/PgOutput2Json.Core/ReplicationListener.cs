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

        /// <summary>
        /// Called on every change of a database row. 
        /// </summary>
        public event MessageHandler? MessageHandler;

        /// <summary>
        /// Called when the replication listener sends an informational message.
        /// </summary>
        public event LoggingHandler? LoggingInfoHandler;

        /// <summary>
        /// Called when the replication listener sends a warning message.
        /// </summary>
        public event LoggingHandler? LoggingWarnHandler;

        /// <summary>
        /// Called on error inside replication listener. 
        /// The listener will automatically try to reconnect after 10 seconds.
        /// </summary>
        public event LoggingErrorHandler? LoggingErrorHandler;

        public event CommitHandler? CommitHandler;

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
                    await using var conn = new LogicalReplicationConnection(_options.ConnectionString);
                    await conn.Open();

                    LoggingInfoHandler?.Invoke("Connected to PostgreSQL");

                    var slot = new PgOutputReplicationSlot(_options.ReplicationSlotName);
                    var replicationOptions = new PgOutputReplicationOptions(_options.PublicationName, 1);

                    DateTime commitTimeStamp = DateTime.UtcNow;

                    await foreach (var message in conn.StartReplication(slot, replicationOptions, cancellationToken))
                    {
                        var confirm = false;

                        if (message is BeginMessage beginMsg)
                        {
                            commitTimeStamp = beginMsg.TransactionCommitTimestamp;
                        }
                        else if (message is InsertMessage insertMsg)
                        {
                            confirm = await WriteTuple(insertMsg.NewRow,
                                             insertMsg.Relation,
                                             "I",
                                             commitTimeStamp,
                                             insertMsg.ServerClock,
                                             _options.WriteNulls);
                        }
                        else if (message is UpdateMessage updateMsg)
                        {
                            confirm = await WriteTuple(updateMsg.NewRow,
                                             updateMsg.Relation,
                                             "U",
                                             commitTimeStamp,
                                             updateMsg.ServerClock,
                                             _options.WriteNulls);
                        }
                        else if (message is KeyDeleteMessage keyDeleteMsg)
                        {
                            confirm = await WriteTuple(keyDeleteMsg.Key,
                                             keyDeleteMsg.Relation,
                                             "D",
                                             commitTimeStamp,
                                             keyDeleteMsg.ServerClock,
                                             false);
                        }
                        else if (message is FullDeleteMessage fullDeleteMsg)
                        {
                            confirm = await WriteTuple(fullDeleteMsg.OldRow,
                                             fullDeleteMsg.Relation,
                                             "D",
                                             commitTimeStamp,
                                             fullDeleteMsg.ServerClock,
                                             false);
                        }
                        else if (message is CommitMessage commitMsg)
                        {
                            CommitHandler?.Invoke();
                            conn.SetReplicationStatus(message.WalEnd);
                        }

                        // Always call SetReplicationStatus() or assign LastAppliedLsn and LastFlushedLsn individually
                        // so that Npgsql can inform the server which WAL files can be removed/recycled.

                        if (confirm) conn.SetReplicationStatus(message.WalEnd);
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
                        LoggingWarnHandler?.Invoke("Slot taken - waiting for 10 seconds...");
                    }
                    else
                    {
                        LoggingErrorHandler?.Invoke(ex, "Error in replication listener. Waiting for 10 seconds...");
                    }

                    Thread.Sleep(10000);
                }
            }
        }

        private async Task<bool> WriteTuple(ReplicationTuple tuple,
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

            if (!_options.KeyColumns.TryGetValue(_tableNameBuilder.ToString(), out var keyColumn))
            {
                keyColumn = null;
            }

            int partition = 0;

            await foreach (var value in tuple)
            {
                var col = relation.Columns[i++];

                if (value.IsDBNull && !sendNulls) continue;

                if (value.IsDBNull || value.Kind == TupleDataKind.TextValue)
                {
                    var isKeyColumn = keyColumn != null && col.ColumnName == keyColumn.ColumnName;

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

                        if (isKeyColumn)
                        {
                            partition = hash % keyColumn!.PartitionCount;
                        }
                    }
                }
            }

            _jsonBuilder.Append('}');

            var confirm = true;
            MessageHandler?.Invoke(_jsonBuilder.ToString(),
                                   _tableNameBuilder.ToString(),
                                   partition,
                                   ref confirm);

            return confirm;
        }
    }
}
