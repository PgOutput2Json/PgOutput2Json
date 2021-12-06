using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;
using System.Text;

namespace Pg2Rabbit.Core
{
    public class ReplicationListener
    {
        public static async Task ListenForChanges(CancellationToken cancellationToken, bool sendNulls)
        {
            while (true)
            {
                try
                {
                    await using var conn = new LogicalReplicationConnection("server=localhost;database=repl_test_db;username=replicator;password=replicator");
                    await conn.Open();

                    Console.WriteLine("Connected to PostgreSQL");

                    var slot = new PgOutputReplicationSlot("test_slot");
                    var options = new PgOutputReplicationOptions("pub_test", 1);

                    var stringBuilder = new StringBuilder(256);

                    DateTime commitTimeStamp = DateTime.UtcNow;

                    await foreach (var message in conn.StartReplication(slot, options, cancellationToken))
                    {
                        if (message is BeginMessage beginMsg)
                        {
                            commitTimeStamp = beginMsg.TransactionCommitTimestamp;
                        }
                        else if (message is InsertMessage insertMsg)
                        {
                            await WriteTuple(stringBuilder,
                                             insertMsg.NewRow,
                                             insertMsg.Relation,
                                             "I",
                                             commitTimeStamp,
                                             insertMsg.ServerClock,
                                             sendNulls);
                        }
                        else if (message is UpdateMessage updateMsg)
                        {
                            await WriteTuple(stringBuilder,
                                             updateMsg.NewRow,
                                             updateMsg.Relation,
                                             "U",
                                             commitTimeStamp,
                                             updateMsg.ServerClock,
                                             sendNulls);
                        }
                        else if (message is KeyDeleteMessage keyDeleteMsg)
                        {
                            await WriteTuple(stringBuilder,
                                             keyDeleteMsg.Key,
                                             keyDeleteMsg.Relation,
                                             "D",
                                             commitTimeStamp,
                                             keyDeleteMsg.ServerClock,
                                             false);
                        }
                        else if (message is FullDeleteMessage fullDeleteMsg)
                        {
                            await WriteTuple(stringBuilder,
                                             fullDeleteMsg.OldRow,
                                             fullDeleteMsg.Relation,
                                             "D",
                                             commitTimeStamp,
                                             fullDeleteMsg.ServerClock,
                                             false);
                        }
                        else if (message is CommitMessage commitMsg)
                        {
                            Console.WriteLine(stringBuilder.ToString());
                            stringBuilder.Clear();
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
                        Console.WriteLine("Slot taken - waiting for 10 seconds...");
                    }
                    else
                    {
                        Console.Error.WriteLine(ex);
                    }

                    Thread.Sleep(10000);
                }
            }
        }

        private static async Task WriteTuple(StringBuilder stringBuilder,
                                             ReplicationTuple tuple,
                                             RelationMessage relation,
                                             string changeType,
                                             DateTime commitTimeStamp,
                                             DateTime messageTimeStamp,
                                             bool sendNulls)
        {
            stringBuilder.Append("{\"_ct\":\"");
            stringBuilder.Append(changeType);
            stringBuilder.Append("\",");

            stringBuilder.Append("\"_cts\":\"");
            stringBuilder.Append(commitTimeStamp.Ticks);
            stringBuilder.Append("\",");

            stringBuilder.Append("\"_mts\":\"");
            stringBuilder.Append(messageTimeStamp.Ticks);
            stringBuilder.Append("\",");

            stringBuilder.Append("\"_re\":\"");
            stringBuilder.Append(relation.Namespace);
            stringBuilder.Append('.');
            stringBuilder.Append(relation.RelationName);
            stringBuilder.Append('"');

            var i = 0;

            await foreach (var value in tuple)
            {
                var col = relation.Columns[i++];

                if (value.IsDBNull && !sendNulls) continue;

                if (value.IsDBNull || value.Kind == TupleDataKind.TextValue)
                {
                    stringBuilder.Append(",\"");
                    stringBuilder.Append(col.ColumnName);
                    stringBuilder.Append("\":");

                    if (value.IsDBNull)
                    {
                        stringBuilder.Append("null");
                    }
                    else if (value.Kind == TupleDataKind.TextValue)
                    {
                        var type = value.GetPostgresType();
                        var pgOid = (PgOid)type.OID;

                        if (pgOid.IsNumber())
                        {
                            JsonUtils.WriteNumber(stringBuilder, value.GetTextReader());
                        }
                        else if (pgOid.IsBoolean())
                        {
                            JsonUtils.WriteBoolean(stringBuilder, value.GetTextReader());
                        }
                        else if (pgOid.IsByte())
                        {
                            JsonUtils.WriteByte(stringBuilder, value.GetTextReader());
                        }
                        else
                        {
                            JsonUtils.WriteText(stringBuilder, value.GetTextReader());
                        }
                    }
                }
            }

            stringBuilder.Append('}');
        }
    }
}
