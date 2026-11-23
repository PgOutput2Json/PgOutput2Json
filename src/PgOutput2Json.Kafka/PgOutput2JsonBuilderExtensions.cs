using System;
using PgOutput2Json.Kafka;

namespace PgOutput2Json
{
    public static class PgOutput2JsonBuilderExtensions
    {
        public static PgOutput2JsonBuilder UseKafka(this PgOutput2JsonBuilder builder, 
            Action<KafkaPublisherOptions>? configureAction = null)
        {
            // the listener waits at the end of a batch until Kafka has acknowledged all of it: 10,000 messages are
            // more than ten times faster than the 100 of the library. WithBatchSize after UseKafka changes it
            builder.WithBatchSize(10_000);

            // it does not know its last position: nothing to skip. WithDeduplication after UseKafka changes it
            builder.WithDeduplication(false);

            var options = new KafkaPublisherOptions();

            configureAction?.Invoke(options);

            builder.WithMessagePublisherFactory(new KafkaPublisherFactory(options));

            return builder;
        }
    }
}
