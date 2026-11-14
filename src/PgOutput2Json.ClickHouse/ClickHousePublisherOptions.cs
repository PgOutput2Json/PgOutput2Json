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
        /// ClickHouse settings sent with every request, e.g. "insert_quorum".
        /// </summary>
        public Dictionary<string, string> Settings { get; } = [];

        /// <summary>
        /// Returns the name of the ClickHouse table for a PostgreSQL table ("schema.table").
        /// By default a table of the public schema keeps its name, and any other becomes "schema_table".
        /// </summary>
        public Func<string, string> GetTableName { get; set; } = DefaultTableName;

        /// <summary>
        /// Optional. The client to use instead of the one the output creates, e.g. with a proxy or a client certificate.
        /// It is not disposed by the output.
        /// </summary>
        public HttpClient? HttpClient { get; set; }

        private static string DefaultTableName(string fullTableName)
        {
            var dot = fullTableName.IndexOf('.');

            if (dot < 0) return fullTableName;

            return fullTableName.AsSpan(0, dot).SequenceEqual("public")
                ? fullTableName[(dot + 1)..]
                : string.Concat(fullTableName.AsSpan(0, dot), "_", fullTableName.AsSpan(dot + 1));
        }
    }
}
