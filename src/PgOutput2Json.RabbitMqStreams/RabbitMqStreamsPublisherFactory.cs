using System;

using Microsoft.Extensions.Logging;

namespace PgOutput2Json.RabbitMqStreams
{
    internal class RabbitMqStreamsPublisherFactory : IMessagePublisherFactory
    {
        private readonly RabbitMqStreamsPublisherOptions _options;

        public RabbitMqStreamsPublisherFactory(RabbitMqStreamsPublisherOptions options)
        {
            _options = options;
        }

        public IMessagePublisher CreateMessagePublisher(ReplicationListenerOptions listenerOptions, string slotName, ILoggerFactory? loggerFactory)
        {
            return new RabbitMqStreamsPublisher(_options, listenerOptions.BatchSize, loggerFactory);
        }

        public void Validate(ReplicationListenerOptions listenerOptions)
        {
            if (!_options.UseDeduplication) return;

            // the last message of a super stream is one per partition
            if (_options.IsSuperStream)
            {
                throw new InvalidOperationException("UseDeduplication works only with a single stream, not with a super stream");
            }

            // the sync workers write to the stream too, with positions of their own slots
            if (listenerOptions.CopyData)
            {
                throw new InvalidOperationException("UseDeduplication cannot be used with the initial data copy");
            }
        }
    }
}
