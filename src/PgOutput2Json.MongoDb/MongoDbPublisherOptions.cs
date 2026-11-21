using System;
using System.Threading.Tasks;

using MongoDB.Driver;

namespace PgOutput2Json.MongoDb
{
    public class MongoDbPublisherOptions
    {
        public required MongoClientSettings ClientSettings { get; set; }
        public required string DatabaseName { get; set; }

        public Func<MongoClient, Task>? PostConnectionSetup { get; set; }

        /// <summary>
        /// The time zone of the values of <c>timestamp</c> (without time zone) columns, which PostgreSQL sends without one.
        /// They are stored as BSON dates (UTC) of that wall time in this zone. The default is UTC: the wall time is stored
        /// as it is, and reads back the same. A <c>timestamptz</c> has its own offset and is not affected.
        /// For example <c>TimeZoneInfo.FindSystemTimeZoneById("Europe/Zagreb")</c>.
        /// </summary>
        public TimeZoneInfo TimestampTimeZone { get; set; } = TimeZoneInfo.Utc;
    }
}
