using Microsoft.Extensions.Logging;

namespace PgOutput2Json.Sqlite
{
    internal class SqlitePublisherFactory : IMessagePublisherFactory
    {
        private readonly SqlitePublisherOptions _options;

        // shared by the publishers: the main stream and the sync workers
        private readonly TargetNames _names;

        public SqlitePublisherFactory(SqlitePublisherOptions options)
        {
            _options = options;
            _names = new TargetNames(options.GetTableName);
        }

        // the compact write mode has the column types, and the JSON is not seen outside the output
        public JsonWriteMode? RequiredWriteMode => JsonWriteMode.Compact;

        // one file has one writer: parallel copy workers would wait for each other's locks
        public bool SupportsParallelCopy => false;

        public IMessagePublisher CreateMessagePublisher(ReplicationListenerOptions listenerOptions, string slotName, ILoggerFactory? loggerFactory)
        {
            return new SqlitePublisher(_options, loggerFactory?.CreateLogger<SqlitePublisher>()) { Names = _names };
        }
    }
}
