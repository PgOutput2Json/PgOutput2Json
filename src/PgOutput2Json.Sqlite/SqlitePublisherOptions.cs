using System;
using System.Threading.Tasks;

using Microsoft.Data.Sqlite;

namespace PgOutput2Json.Sqlite
{
    public enum WalCheckpointType
    {
        /// <summary>
        /// Default. Wal chackpoint is not forced (automatic checkpoint still works)
        /// </summary>
        Automatic = 0,

        Passive = 1,
        Restart = 2,
        Truncate = 3,
        Full = 4,
    }

    public class SqlitePublisherOptions
    {
        public required SqliteConnectionStringBuilder ConnectionStringBuilder { get; set; }
        public Func<SqliteConnection, Task>? PostConnectionSetup { get; set; }

        public bool UseWal { get; set; }
        public WalCheckpointType WalCheckpointType { get; set; } = WalCheckpointType.Automatic;
        public int WalCheckpointTryCount { get; set; } = 10;
    }
}
