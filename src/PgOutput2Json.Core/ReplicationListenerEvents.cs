namespace PgOutput2Json.Core
{
    /// <summary>
    /// Handles json message stored in the json StringBuilder. 
    /// StringBuilders contents can be modified in the handler.
    /// </summary>
    /// <param name="json">JSON representation of the changed row.</param>
    /// <param name="tableName">Schema qualified table name</param>
    /// <param name="keyColumnValue">
    /// The value of the key column, if configured in <see cref="ReplicationListenerOptions.KeyColumns"/>.
    /// If the key kolumn is not configured, this will be set to empty string.
    /// </param>
    /// <param name="partition">Partition number in range from 0 to partition count - 1</param>
    /// <param name="confirm">If set to false, the message processing will not be confirmed to the db</param>
    public delegate void MessageHandler(string json, string tableName, string keyColumnValue, int partition, ref bool confirm);

    /// <summary>
    /// This is called by the listener periodically (5 secs default) if there are messages that should be confirmed
    /// </summary>
    public delegate void ConfirmHandler();
}
