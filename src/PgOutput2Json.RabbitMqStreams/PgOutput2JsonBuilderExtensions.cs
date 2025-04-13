using System;
using System.Net;

namespace PgOutput2Json.RabbitMqStreams
{
    public static class PgOutput2JsonBuilderExtensions
    {
        public static PgOutput2JsonBuilder UseRabbitMqStreams(this PgOutput2JsonBuilder builder, 
            Action<RabbitMqStreamsPublisherOptions>? configureAction = null)
        {
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
