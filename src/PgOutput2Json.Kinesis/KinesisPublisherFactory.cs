using Microsoft.Extensions.Logging;

namespace PgOutput2Json.Kinesis
{
    internal class KinesisPublisherFactory : IMessagePublisherFactory
    {
        private readonly KinesisPublisherOptions _options;

        public KinesisPublisherFactory(KinesisPublisherOptions options)
        {
            _options = options;
        }

        public IMessagePublisher CreateMessagePublisher(ReplicationListenerOptions listenerOptions, string slotName, ILoggerFactory? loggerFactory)
        {
            if (listenerOptions.BatchSize > 500) throw new System.Exception("BatchSize cannot be larger than 500 for KinesisPublisher");

            return new KinesisPublisher(_options, loggerFactory?.CreateLogger<KinesisPublisher>());
        }
    }
}
