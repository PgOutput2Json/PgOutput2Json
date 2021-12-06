using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;
using System.Text;

namespace Pg2Rabbit.Core
{
    public class ReplicationListener
    {
        public static async Task ListenForChanges()
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

                    // The following will loop until the cancellation token is triggered, and will print message types coming from PostgreSQL:
                    var cancellationTokenSource = new CancellationTokenSource();

                    await foreach (var message in conn.StartReplication(slot, options, cancellationTokenSource.Token))
                    {
                        stringBuilder.Clear();

                        Console.WriteLine($"Received message type: {message.GetType().Name}");

                        if (message is InsertMessage insertMsg)
                        {
                            Console.WriteLine($"Insert into {insertMsg.Relation.RelationName}:");

                            await WriteTuple(stringBuilder, insertMsg.NewRow);

                            Console.WriteLine(stringBuilder.ToString());
                        }
                        if (message is UpdateMessage updateMsg)
                        {
                            Console.WriteLine($"Update in {updateMsg.Relation.RelationName}:");

                            await WriteTuple(stringBuilder, updateMsg.NewRow);

                            Console.WriteLine(stringBuilder.ToString());
                        }
                                 
                        // Always call SetReplicationStatus() or assign LastAppliedLsn and LastFlushedLsn individually
                        // so that Npgsql can inform the server which WAL files can be removed/recycled.

                        conn.SetReplicationStatus(message.WalEnd);
                    }
                }
                catch (OperationCanceledException)
                {
                    // We are reconnecting every X seconds - not sure if needed
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

        private static async Task WriteTuple(StringBuilder stringBuilder, ReplicationTuple tuple)
        {
            stringBuilder.Append("[");

            await foreach (var value in tuple)
            {
                switch (value.Kind)
                {
                    case TupleDataKind.Null:
                        if (stringBuilder.Length > 1) stringBuilder.Append(',');
                        stringBuilder.Append("null");
                        break;
                    case TupleDataKind.TextValue:
                        if (stringBuilder.Length > 1) stringBuilder.Append(',');

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
                        break;
                }
            }

            stringBuilder.Append("]");
        }
    }
}
