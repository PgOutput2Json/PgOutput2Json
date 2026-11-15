using PgOutput2Json.RabbitMqStreams;
using System;
using System.Net;

namespace PgOutput2Json
{
    public static class PgOutput2JsonBuilderExtensions
    {
        public static PgOutput2JsonBuilder UseRabbitMqStreams(this PgOutput2JsonBuilder builder, 
            Action<RabbitMqStreamsPublisherOptions>? configureAction = null)
        {
            // the producer has a batch in flight, and the listener waits for its confirmations at the end of the batch:
            // 10,000 messages are about 2.5 times faster than the 100 of the library. WithBatchSize after UseRabbitMqStreams changes it
            builder.WithBatchSize(10_000);

            var options = new RabbitMqStreamsPublisherOptions();

            configureAction?.Invoke(options);

            if (options.StreamSystemConfig.Endpoints.Count == 0)
            {
                options.StreamSystemConfig.Endpoints.Add(new IPEndPoint(IPAddress.Loopback, 5552));
            }

            builder.WithMessagePublisherFactory(new RabbitMqStreamsPublisherFactory(options));

            return builder;
        }
    }
}
