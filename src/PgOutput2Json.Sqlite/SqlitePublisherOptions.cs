using Microsoft.Data.Sqlite;

namespace PgOutput2Json.Sqlite
{
    public class SqlitePublisherOptions
    {
        public required SqliteConnectionStringBuilder ConnectionStringBuilder { get; set; }
    }
}
