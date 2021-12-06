using System.Text;

namespace PgOutput2Json.Core
{
    /// <summary>
    /// Handles json message stored in the json StringBuilder. 
    /// StringBuilders contents can be modified in the handler.
    /// The handler should return true if the message is successfully processed.
    /// </summary>
    /// <param name="json">StringBuilder containing json representation of the changed row.</param>
    /// <param name="tableName">Schema qualified table name</param>
    /// <param name="partition">Partition number in range from 0 to partition count - 1</param>
    public delegate bool MessageHandler(StringBuilder json, StringBuilder tableName, int partition);

    public delegate void LoggingHandler(string logMessage);
    public delegate void LoggingErrorHandler(Exception ex, string logMessage);

    public class ReplicationListenerOptions
    {
        public string ConnectionString { get; set; }
        public string PublicationName { get; set; }
        public string ReplicationSlotName { get; set; }

        public Dictionary<string, PartionConfig> Partitions { get; set; } 
            = new Dictionary<string, PartionConfig>();

        /// <summary>
        /// Should nulls be written to JSON output. Default is false.
        /// </summary>
        public bool WriteNulls { get; set; } = false;

        /// <summary>
        /// Called on every change of a database row. 
        /// </summary>
        public MessageHandler MessageHandler { get; set; }

        /// <summary>
        /// Called when the replication listener sends an informational message.
        /// </summary>
        public LoggingHandler? LoggingInfoHandler { get; set; }

        /// <summary>
        /// Called when the replication listener sends a warning message.
        /// </summary>
        public LoggingHandler? LoggingWarnHandler { get; set; }

        /// <summary>
        /// Called on error inside replication listener. 
        /// The listener will automatically try to reconnect after 10 seconds.
        /// </summary>
        public LoggingErrorHandler? LoggingErrorHandler { get; set; }

        public ReplicationListenerOptions(string connectionString,
                                          string publicationName,
                                          string replicationSlotName,
                                          MessageHandler messageHandler)
        {
            ConnectionString = connectionString;
            PublicationName = publicationName;
            ReplicationSlotName = replicationSlotName;
            MessageHandler = messageHandler;
        }
    }
}
