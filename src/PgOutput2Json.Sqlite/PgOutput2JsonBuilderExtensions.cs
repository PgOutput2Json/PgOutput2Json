using Microsoft.Data.Sqlite;
using PgOutput2Json.Sqlite;
using System;

namespace PgOutput2Json
{
    public static class PgOutput2JsonBuilderExtensions
    {
        public static PgOutput2JsonBuilder UseSqlite(this PgOutput2JsonBuilder builder,
                                                     Action<SqlitePublisherOptions>? configureAction = null)
        {
            // a batch is one transaction: 1,000 rows are about 10% faster than the 100 of the library, and still short;
            // WithBatchSize after UseSqlite changes it
            builder.WithBatchSize(1000);

            var options = new SqlitePublisherOptions 
            { 
                ConnectionStringBuilder = new SqliteConnectionStringBuilder() 
            };

            options.ConnectionStringBuilder.DataSource = "pgoutput2json.s3db";
            options.ConnectionStringBuilder.Mode = SqliteOpenMode.ReadWriteCreate;

            configureAction?.Invoke(options);

            if (string.IsNullOrWhiteSpace(options.SubscriptionName))
            {
                throw new ArgumentException("SubscriptionName must be provided: the store keeps the position of each subscription under its name", nameof(options.SubscriptionName));
            }

            builder.WithMessagePublisherFactory(new SqlitePublisherFactory(options));

            return builder;
        }
    }
}
