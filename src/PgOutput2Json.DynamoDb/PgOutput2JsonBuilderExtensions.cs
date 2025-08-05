using Amazon.DynamoDBv2;
using PgOutput2Json.DynamoDb;
using System;

namespace PgOutput2Json
{
    public static class PgOutput2JsonBuilderExtensions
    {
        public static PgOutput2JsonBuilder UseDynamoDb(this PgOutput2JsonBuilder builder, Action<DynamoDbPublisherOptions>? configureAction = null)
        {
            // Mongo publisher only supports compact mode (for now)
            builder.WithJsonOptions(options => options.WriteMode = JsonWriteMode.Compact);
            builder.WithBatchSize(25); // max for DynamoDB

            var options = new DynamoDbPublisherOptions
            {
                ClientConfig = new AmazonDynamoDBConfig
                {
                    ServiceURL = "http://localhost:8000",  // for local DynamoDB, change/remove for AWS
                    MaxErrorRetry = 3,                     // retry failed requests up to 3 times
                },
            };

            configureAction?.Invoke(options);

            builder.WithMessagePublisherFactory(new DynamoDbPublisherFactory(options));

            return builder;
        }
    }
}
