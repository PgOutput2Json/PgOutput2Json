using System;
using System.Collections.Generic;
using System.Net.Http;

namespace PgOutput2Json.ClickHouse
{
    public class ClickHousePublisherOptions
    {
        /// <summary>
        /// The HTTP interface of ClickHouse. Default is http://localhost:8123
        /// </summary>
        public string Endpoint { get; set; } = "http://localhost:8123";

        /// <summary>
        /// The database the tables are created in. It must exist. Default is "default".
        /// </summary>
        public string Database { get; set; } = "default";

        public string Username { get; set; } = "default";
        public string Password { get; set; } = "";

        /// <summary>
        /// With asynchronous inserts ClickHouse collects the rows of several inserts before it writes them, which
        /// keeps the number of parts low when the batches are small. The insert is still acknowledged only when
        /// the rows are written (wait_for_async_insert). Default is true.
        /// </summary>
        public bool UseAsyncInserts { get; set; } = true;

        /// <summary>
        /// Stores json and jsonb columns in the JSON type of ClickHouse (version 25.3 or newer). The JSON type holds
        /// only objects, so any other value (an array, a number, a string, a JSON null) is stored as {"v": value}.
        /// With false the columns are Strings with the JSON text. Default is true.
        /// </summary>
        public bool UseJsonType { get; set; } = true;

        /// <summary>
        /// Stores the arrays of numbers, booleans, text, dates, timestamps and uuids in ClickHouse arrays, e.g.
        /// Array(Nullable(Int32)). ClickHouse arrays cannot be null, so a null array is stored as an empty one, and
        /// they have a fixed number of dimensions: a multi-dimensional value stops the replication with an error.
        /// With false, or for arrays of other types, the columns are Strings with the JSON text. Default is true.
        /// </summary>
        public bool UseArrayTypes { get; set; } = true;

        /// <summary>
        /// Sends the rows in the RowBinary format, which ClickHouse reads two to three times faster than JSON. The output then
        /// writes the values as ClickHouse would parse their text (time zones, dates out of range, decimal digits). A table with
        /// a column of a type RowBinary does not cover (e.g. Map, Tuple) still gets JSON. Default is false: every table gets
        /// JSON (JSONCompactEachRow), which ClickHouse parses itself.
        /// </summary>
        public bool UseRowBinary { get; set; }

        /// <summary>
        /// The time zone of the values of <c>timestamp</c> (without time zone) columns, which PostgreSQL sends without one.
        /// The output creates their columns as <c>DateTime64(6, '&lt;zone&gt;')</c>, so ClickHouse stores the moment of
        /// that wall time in this zone, never in the time zone of the server. Default is UTC. Columns that already exist
        /// are not changed: a <c>DateTime64</c> without a time zone (created by versions before 2.2) keeps the time zone of
        /// the server. A <c>timestamptz</c> has its own offset and is not affected.
        /// For example <c>TimeZoneInfo.FindSystemTimeZoneById("Europe/Zagreb")</c>.
        /// </summary>
        public TimeZoneInfo TimestampTimeZone { get; set; } = TimeZoneInfo.Utc;

        /// <summary>
        /// Removes the rows of deleted PostgreSQL rows from ClickHouse when no new rows have arrived in a table for this
        /// long. Without it a deleted row is kept as a row marked deleted, which FINAL hides but which still takes
        /// space. It uses the cleanup merges of ReplacingMergeTree, which ClickHouse still marks experimental.
        /// The settings are added to the tables on the next start, also to existing tables. Default is null: deleted
        /// rows are kept.
        /// </summary>
        public TimeSpan? CleanUpDeletedRowsAfter { get; set; }

        /// <summary>
        /// The ORDER BY, PARTITION BY and engine of the ClickHouse table of a PostgreSQL table, by its name ("schema.table").
        /// They are used only when the output creates the table. A table that is not listed gets the defaults.
        /// </summary>
        public Dictionary<string, ClickHouseTableOptions> Tables { get; } = [];

        /// <summary>
        /// ClickHouse settings sent with every request, e.g. "insert_quorum".
        /// </summary>
        public Dictionary<string, string> Settings { get; } = [];

        /// <summary>
        /// Returns the name of the ClickHouse table for a PostgreSQL table, from its schema and its name.
        /// By default a table of the public schema keeps its name, and any other becomes "schema_table".
        /// Two PostgreSQL tables with the same ClickHouse name are an error.
        /// </summary>
        public Func<string, string, string> GetTableName { get; set; } = (schemaName, tableName) => TargetNames.Default(schemaName, tableName, "_");

        /// <summary>
        /// Optional. The client to use instead of the one the output creates, e.g. with a proxy or a client certificate.
        /// It is not disposed by the output.
        /// </summary>
        public HttpClient? HttpClient { get; set; }
    }
}
