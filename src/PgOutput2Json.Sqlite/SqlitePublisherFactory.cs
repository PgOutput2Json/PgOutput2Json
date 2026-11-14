using Microsoft.Extensions.Logging;

namespace PgOutput2Json.Sqlite
{
    internal class SqlitePublisherFactory : IMessagePublisherFactory
    {
        private readonly SqlitePublisherOptions _options;

        public SqlitePublisherFactory(SqlitePublisherOptions options)
        {
            _options = options;
        }

        // the compact write mode has the column types, and the JSON is not seen outside the output
        public JsonWriteMode? RequiredWriteMode => JsonWriteMode.Compact;

        public IMessagePublisher CreateMessagePublisher(ReplicationListenerOptions listenerOptions, string slotName, ILoggerFactory? loggerFactory)
        {
            return new SqlitePublisher(_options, loggerFactory?.CreateLogger<SqlitePublisher>());
        }
    }
}
