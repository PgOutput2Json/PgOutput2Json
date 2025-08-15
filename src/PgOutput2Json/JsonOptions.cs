namespace PgOutput2Json
{
    public enum JsonWriteMode
    {
        Default = 0,
        Compact = 1,
    }

    public enum TimestampFormat
    {
        DateTimeTicks = 0,
        UnixTimeMilliseconds = 1,
    }

    public class JsonOptions
    {
        /// <summary>
        /// If true, the null values will be written in JSON. Default is false
        /// </summary>
        public bool WriteNulls { get; set; }

        /// <summary>
        /// If true, the resulting JSON will contain extra attributes:
        /// - "_cts": commit timestamp in UTC ticks
        /// - "_mts": message timestamp in UTC ticks
        /// Default is false.
        /// </summary>
        public bool WriteTimestamps { get; set; }

        /// <summary>
        /// Which format to write the timestamps in (default is DateTimeTicks)
        /// </summary>
        public TimestampFormat TimestampFormat { get; set; } = TimestampFormat.DateTimeTicks;

        /// <summary>
        /// If true, table name will be written in an additional attribute:
        /// - "_tbl": Schema qualified table name
        /// Default is false (RabbitMQ is sending the table name as "messageType").
        /// </summary>
        public bool WriteTableNames { get; set; }

        /// <summary>
        /// If set to Compact, the JSON values will be written as arrays, without column names.
        /// Column names will be sent in a separate "s" (schema) property, when a Relation message is encountered.
        /// </summary>
        public JsonWriteMode WriteMode { get; set; } = JsonWriteMode.Default;
    }
}
