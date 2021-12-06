using System.Text;

namespace PgOutput2Json.Core
{
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
        /// The first parameter is json, the second parameter is table name, and the third parameter is partion.
        /// Partitions are ints from 0 to partition count - 1.
        /// </summary>
        public Action<StringBuilder, StringBuilder, int> MessageHandler { get; set; }

        /// <summary>
        /// Called when the replication listener sends an informational message.
        /// </summary>
        public Action<string>? LoggingInfoHandler { get; set; }

        /// <summary>
        /// Called when the replication listener sends a warning message.
        /// </summary>
        public Action<string>? LoggingWarnHandler { get; set; }

        /// <summary>
        /// Called on error inside replication listener. 
        /// The listener will automatically try to reconnect after 10 seconds.
        /// </summary>
        public Action<Exception, string>? LoggingErrorHandler { get; set; }

        public ReplicationListenerOptions(string connectionString,
                                          string publicationName,
                                          string replicationSlotName,
                                          Action<StringBuilder, StringBuilder, int> messageHandler)
        {
            ConnectionString = connectionString;
            PublicationName = publicationName;
            ReplicationSlotName = replicationSlotName;
            MessageHandler = messageHandler;
        }
    }
}
