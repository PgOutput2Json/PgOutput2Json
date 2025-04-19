﻿using System;

namespace PgOutput2Json.Kafka
{
    public static class PgOutput2JsonBuilderExtensions
    {
        public static PgOutput2JsonBuilder UseKafka(this PgOutput2JsonBuilder builder, 
            Action<KafkaPublisherOptions>? configureAction = null)
        {
            var options = new KafkaPublisherOptions();

            configureAction?.Invoke(options);

            builder.WithMessagePublisherFactory(new KafkaPublisherFactory(options));

            return builder;
        }
    }
}
