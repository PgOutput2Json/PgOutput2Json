using MongoDB.Driver;
using PgOutput2Json.MongoDb;
using System;

namespace PgOutput2Json
{
    public static class PgOutput2JsonBuilderExtensions
    {
        public static PgOutput2JsonBuilder UseMongoDb(this PgOutput2JsonBuilder builder,
                                                      Action<MongoDbPublisherOptions>? configureAction = null)
        {
            var options = new MongoDbPublisherOptions
            {
                ClientSettings = new MongoClientSettings
                {
                    Server = new MongoServerAddress("localhost", 27017),
                    Scheme = MongoDB.Driver.Core.Configuration.ConnectionStringScheme.MongoDB,
                },
                DatabaseName = "pg_output2json",
            };

            configureAction?.Invoke(options);

            if (string.IsNullOrWhiteSpace(options.SubscriptionName))
            {
                throw new ArgumentException("SubscriptionName must be provided: the store keeps the position of each subscription under its name", nameof(options.SubscriptionName));
            }

            builder.WithMessagePublisherFactory(new MongoDbPublisherFactory(options));

            return builder;
        }
    }
}
