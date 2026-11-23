using System;
using PgOutput2Json.Kinesis;

namespace PgOutput2Json
{
    public static class PgOutput2JsonBuilderExtensions
    {
        public static PgOutput2JsonBuilder UseKinesis(this PgOutput2JsonBuilder builder, 
            Action<KinesisPublisherOptions>? configureAction = null)
        {
            // it does not know its last position: nothing to skip. WithDeduplication after UseKinesis changes it
            builder.WithDeduplication(false);

            var options = new KinesisPublisherOptions();

            configureAction?.Invoke(options);

            builder.WithMessagePublisherFactory(new KinesisPublisherFactory(options));

            return builder;
        }
    }
}
