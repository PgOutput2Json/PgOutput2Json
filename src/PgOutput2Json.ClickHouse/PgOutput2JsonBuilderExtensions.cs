using System;

using PgOutput2Json.ClickHouse;

namespace PgOutput2Json
{
    public static class PgOutput2JsonBuilderExtensions
    {
        public static PgOutput2JsonBuilder UseClickHouse(this PgOutput2JsonBuilder builder,
                                                         Action<ClickHousePublisherOptions>? configureAction = null)
        {
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

            if (options.CleanUpDeletedRowsAfter is { } cleanUpAfter && cleanUpAfter < TimeSpan.FromSeconds(1))
            {
                throw new ArgumentException("ClickHouse CleanUpDeletedRowsAfter must be at least one second", nameof(options.CleanUpDeletedRowsAfter));
            }

            builder.WithMessagePublisherFactory(new ClickHousePublisherFactory(options));

            return builder;
        }
    }
}
