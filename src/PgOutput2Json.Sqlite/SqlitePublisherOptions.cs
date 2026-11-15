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
        /// <summary>
        /// The database to write to. The output opens a connection of its own, without pooling,
        /// and turns off the foreign key checks on it.
        /// </summary>
        public required SqliteConnectionStringBuilder ConnectionStringBuilder { get; set; }

        /// <summary>
        /// Runs after the output has opened its connection and set it up, before the first write.
        /// </summary>
        public Func<SqliteConnection, Task>? PostConnectionSetup { get; set; }

        public bool UseWal { get; set; }
        public WalCheckpointType WalCheckpointType { get; set; } = WalCheckpointType.Automatic;
        public int WalCheckpointTryCount { get; set; } = 10;
    }
}
