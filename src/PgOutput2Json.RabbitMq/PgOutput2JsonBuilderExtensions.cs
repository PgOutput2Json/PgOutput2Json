using PgOutput2Json.RabbitMq;
using System;

namespace PgOutput2Json
{
    public static class PgOutput2JsonBuilderExtensions
    {
        public static PgOutput2JsonBuilder UseRabbitMq(this PgOutput2JsonBuilder builder, 
            Action<RabbitMqPublisherOptions>? configureAction = null)
        {
            var options = new RabbitMqPublisherOptions();

            configureAction?.Invoke(options);

            if (options.HostNames.Count == 0)
            {
                options.HostNames.Add(options.ConnectionFactory.HostName);
            }

            builder.WithMessagePublisherFactory(new RabbitMqPublisherFactory(options));

            return builder;
        }
    }
}
