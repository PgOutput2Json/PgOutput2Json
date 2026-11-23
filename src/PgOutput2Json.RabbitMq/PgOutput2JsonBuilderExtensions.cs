using PgOutput2Json.RabbitMq;
using System;

namespace PgOutput2Json
{
    public static class PgOutput2JsonBuilderExtensions
    {
        public static PgOutput2JsonBuilder UseRabbitMq(this PgOutput2JsonBuilder builder, 
            Action<RabbitMqPublisherOptions>? configureAction = null)
        {
            // the listener waits at the end of a batch for the confirms of RabbitMQ: 1,000 messages are about 30% faster
            // than the 100 of the library, more are not. WithBatchSize after UseRabbitMq changes it
            builder.WithBatchSize(1000);

            // it does not know its last position: nothing to skip. WithDeduplication after UseRabbitMq changes it
            builder.WithDeduplication(false);

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
