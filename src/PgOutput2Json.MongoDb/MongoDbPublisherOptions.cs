using System;
using System.Threading.Tasks;

using MongoDB.Driver;

namespace PgOutput2Json.MongoDb
{
    public class MongoDbPublisherOptions
    {
        public required MongoClientSettings ClientSettings { get; set; }
        public required string DatabaseName { get; set; }

        /// <summary>
        /// Required. The name of this subscription (one source database and replication slot) in the store: the position
        /// of the last saved change is kept under it. Every source that writes into the same store needs its own.
        /// </summary>
        public string SubscriptionName { get; set; } = "";

        public Func<MongoClient, Task>? PostConnectionSetup { get; set; }

        /// <summary>
        /// The time zone of the values of <c>timestamp</c> (without time zone) columns, which PostgreSQL sends without one.
        /// They are stored as BSON dates (UTC) of that wall time in this zone. The default is UTC: the wall time is stored
        /// as it is, and reads back the same. A <c>timestamptz</c> has its own offset and is not affected.
        /// For example <c>TimeZoneInfo.FindSystemTimeZoneById("Europe/Zagreb")</c>.
        /// </summary>
        public TimeZoneInfo TimestampTimeZone { get; set; } = TimeZoneInfo.Utc;

        /// <summary>
        /// Returns the name of the MongoDB collection for a PostgreSQL table, from its schema and its name.
        /// By default a table of the public schema keeps its name, and any other becomes "schema.table".
        /// Two PostgreSQL tables with the same collection name are an error.
        /// </summary>
        public Func<string, string, string> GetTableName { get; set; } = (schemaName, tableName) => TargetNames.Default(schemaName, tableName, ".");
    }
}
