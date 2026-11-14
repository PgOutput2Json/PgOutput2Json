using Microsoft.Extensions.Logging;

namespace PgOutput2Json.MongoDb
{
    internal class MongoDbPublisherFactory : IMessagePublisherFactory
    {
        private readonly MongoDbPublisherOptions _options;

        public MongoDbPublisherFactory(MongoDbPublisherOptions options)
        {
            _options = options;
        }

        // the compact write mode has the column types, and the JSON is not seen outside the output
        public JsonWriteMode? RequiredWriteMode => JsonWriteMode.Compact;

        public IMessagePublisher CreateMessagePublisher(ReplicationListenerOptions listenerOptions, string slotName, ILoggerFactory? loggerFactory)
        {
            return new MongoDbPublisher(_options, loggerFactory?.CreateLogger<MongoDbPublisher>(), listenerOptions.UseDeduplication);
        }
    }
}
