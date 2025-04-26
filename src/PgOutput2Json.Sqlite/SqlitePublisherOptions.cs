using System;
using System.Threading.Tasks;

using Microsoft.Data.Sqlite;

namespace PgOutput2Json.Sqlite
{
    public class SqlitePublisherOptions
    {
        public required SqliteConnectionStringBuilder ConnectionStringBuilder { get; set; }
        public Func<SqliteConnection, Task>? PostConnectionSetup { get; set; }
    }
}
