namespace PgOutput2Json.Core
{
    public class ReplicationListenerOptions
    {
        /// <summary>
        /// Should nulls be written to JSON output. Default is false.
        /// </summary>
        public bool WriteNulls { get; set; } = false;

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
    }
}
