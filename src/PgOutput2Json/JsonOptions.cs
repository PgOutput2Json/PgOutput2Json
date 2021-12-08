namespace PgOutput2Json
{
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
        /// If true, table name will be written in an additional attribute:
        /// - "_tbl": Schema qualified table name
        /// Default is false (RabbitMQ is sending the table name as "messageType").
        /// </summary>
        public bool WriteTableNames { get; set; }
    }
}
