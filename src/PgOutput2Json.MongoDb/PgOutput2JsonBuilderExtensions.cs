using MongoDB.Driver;
using System;

namespace PgOutput2Json.MongoDb
{
    public static class PgOutput2JsonBuilderExtensions
    {
        public static PgOutput2JsonBuilder UseMongoDb(this PgOutput2JsonBuilder builder,
                                                      Action<MongoDbPublisherOptions>? configureAction = null)
        {
            // Mongo publisher only supports compact mode (for now)
            builder.WithJsonOptions(options => options.WriteMode = JsonWriteMode.Compact);

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

            builder.WithMessagePublisherFactory(new MongoDbPublisherFactory(options));

            return builder;
        }
    }
}
