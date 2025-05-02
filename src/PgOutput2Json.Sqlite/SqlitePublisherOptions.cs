using System;
using System.Threading.Tasks;

using Microsoft.Data.Sqlite;

namespace PgOutput2Json.Sqlite
{
    public enum WalCheckpointType
    {
        Full = 0,
        Restart = 1,
        Truncate = 2,
    }

    public class SqlitePublisherOptions
    {
        public required SqliteConnectionStringBuilder ConnectionStringBuilder { get; set; }
        public Func<SqliteConnection, Task>? PostConnectionSetup { get; set; }

        public bool UseWal { get; set; }
        public WalCheckpointType WalCheckpointType { get; set; } = WalCheckpointType.Full;
        public int WalCheckpointTryCount { get; set; } = 10;
    }
}
