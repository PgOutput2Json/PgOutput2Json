using System;

using PgOutput2Json.ClickHouse;

namespace PgOutput2Json
{
    public static class PgOutput2JsonBuilderExtensions
    {
        public static PgOutput2JsonBuilder UseClickHouse(this PgOutput2JsonBuilder builder,
                                                         Action<ClickHousePublisherOptions>? configureAction = null)
        {
            // the compact write mode has the column types, which the tables are created from
            builder.WithJsonOptions(options => options.WriteMode = JsonWriteMode.Compact);

            // ClickHouse wants few, large inserts - WithBatchSize after UseClickHouse changes it
            builder.WithBatchSize(10_000);

            var options = new ClickHousePublisherOptions();

            configureAction?.Invoke(options);

            if (string.IsNullOrEmpty(options.Endpoint))
            {
                throw new ArgumentException("ClickHouse Endpoint must be provided", nameof(options.Endpoint));
            }

            if (string.IsNullOrEmpty(options.Database))
            {
                throw new ArgumentException("ClickHouse Database must be provided", nameof(options.Database));
            }

            builder.WithMessagePublisherFactory(new ClickHousePublisherFactory(options));

            return builder;
        }
    }
}
