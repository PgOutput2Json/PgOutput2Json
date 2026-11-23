using Microsoft.Extensions.Logging;

namespace PgOutput2Json.DynamoDb
{
    internal class DynamoDbPublisherFactory : IMessagePublisherFactory
    {
        private readonly DynamoDbPublisherOptions _options;

        // shared by the publishers: the main stream and the sync workers
        private readonly TargetNames _names;

        public DynamoDbPublisherFactory(DynamoDbPublisherOptions options)
        {
            _options = options;
            _names = new TargetNames(options.GetTableName);
        }

        // the compact write mode has the column types, and the JSON is not seen outside the output
        public JsonWriteMode? RequiredWriteMode => JsonWriteMode.Compact;

        public IMessagePublisher CreateMessagePublisher(ReplicationListenerOptions listenerOptions, string slotName, ILoggerFactory? loggerFactory)
        {
            return new DynamoDbPublisher(_options, loggerFactory?.CreateLogger<DynamoDbPublisher>()) { Names = _names };
        }
    }
}

