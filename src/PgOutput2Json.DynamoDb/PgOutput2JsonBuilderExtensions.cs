using Amazon.DynamoDBv2;
using PgOutput2Json.DynamoDb;
using System;

namespace PgOutput2Json
{
    public static class PgOutput2JsonBuilderExtensions
    {
        public static PgOutput2JsonBuilder UseDynamoDb(this PgOutput2JsonBuilder builder, Action<DynamoDbPublisherOptions>? configureAction = null)
        {
            // the items go in batch writes of 25 (the maximum of DynamoDB), several of them at the same time,
            // and the position is saved once a batch. WithBatchSize after UseDynamoDb changes it
            builder.WithBatchSize(1000);

            var options = new DynamoDbPublisherOptions
            {
                ClientConfig = new AmazonDynamoDBConfig
                {
                    ServiceURL = "http://localhost:8000",  // for local DynamoDB, change/remove for AWS
                    MaxErrorRetry = 3,                     // retry failed requests up to 3 times
                },
            };

            configureAction?.Invoke(options);

            if (string.IsNullOrWhiteSpace(options.SubscriptionName))
            {
                throw new ArgumentException("SubscriptionName must be provided: the store keeps the position of each subscription under its name", nameof(options.SubscriptionName));
            }

            builder.WithMessagePublisherFactory(new DynamoDbPublisherFactory(options));

            return builder;
        }
    }
}
