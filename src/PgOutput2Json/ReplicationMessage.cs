using Npgsql.Replication.PgOutput.Messages;
using System;

namespace PgOutput2Json
{
    public class ReplicationMessage
    {
        public ulong WalSeqNo { get; set; }
        public PgOutputReplicationMessage? Message { get; set; }
        public DateTime CommitTimeStamp { get; set; }
        public bool HasRelationChanged { get; set; }
    }
}
