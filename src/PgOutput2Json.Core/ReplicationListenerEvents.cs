using System.Text;

namespace PgOutput2Json.Core
{
    /// <summary>
    /// Handles json message stored in the json StringBuilder. 
    /// StringBuilders contents can be modified in the handler.
    /// </summary>
    /// <param name="json">StringBuilder containing json representation of the changed row.</param>
    /// <param name="tableName">Schema qualified table name</param>
    /// <param name="partition">Partition number in range from 0 to partition count - 1</param>
    /// <param name="confirm">If set to false, the message processing will not be confirmed to the db</param>
    public delegate void MessageHandler(StringBuilder json, StringBuilder tableName, int partition, ref bool confirm);

    public delegate void LoggingHandler(string logMessage);
    public delegate void LoggingErrorHandler(Exception ex, string logMessage);

    public delegate void CommitHandler();
}
