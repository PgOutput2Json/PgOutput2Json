---
title: Azure Event Hubs
description: Publish PostgreSQL changes to Azure Event Hubs with PgOutput2Json.AzureEventHubs.
---

PgOutput2Json supports pushing row changes as JSON messages to Azure Event Hubs.

> **Important:** First set up PostgreSQL as described in [Getting started](../../getting-started/).

## Connection String

You can obtain the Event Hubs connection string from the Azure portal:

1. Navigate to your Event Hubs namespace
2. Go to "Shared access policies"
3. Select or create a policy with "Send" permissions
4. Copy the connection string

## Event Hub Creation

Make sure to create the Event Hub in your namespace before running the application. You can do this through:

- Azure portal
- Azure CLI: `az eventhubs eventhub create --resource-group myResourceGroup --namespace-name myNamespace --name my-eventhub`
- ARM templates
- Terraform

## Partitioning Strategy

The publisher chooses the partition of every event itself (murmur2, as the Kafka output does). By default it hashes the table name, so all the rows of a table go to one partition, in the order of the changes, and the tables are spread over the partitions. With partition-key columns configured for a table (`WithPartitionKeyFields`), it hashes the values of those columns instead, so the rows of the table are spread over the partitions.

At the end of every batch the events are sent once per partition, and the partitions are sent at the same time. Each partition gets its events in WAL order, also the events of different tables.

Up to version 2.0.1, the partition of a table without partition-key columns was chosen by Event Hubs, from the table name as the partition key. After an upgrade, a table can therefore move to another partition. A consumer that keeps a watermark per table and partition must take the watermark of the table from its old partition.

## Create a .NET Worker Service

Set up a new **.NET Worker Service** and add the following package reference:

```
dotnet add package PgOutput2Json.AzureEventHubs
```

In your `Worker.cs`, use the following code to configure change propagation to Azure Event Hubs:

```csharp
using PgOutput2Json;

public class Worker : BackgroundService
{
    private readonly ILoggerFactory _loggerFactory;

    public Worker(ILoggerFactory loggerFactory)
    {
        _loggerFactory = loggerFactory;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // This code assumes PostgreSQL is running on localhost
        using var pgOutput2Json = PgOutput2JsonBuilder.Create()
            .WithLoggerFactory(_loggerFactory)
            .WithPgConnectionString("server=localhost;database=my_database;username=pgoutput2json;password=_your_password_here_")
            .WithPgPublications("my_publication")
            .UseEventHubs(options =>
            {
                // Connection string format: Endpoint=sb://[namespace].servicebus.windows.net/;SharedAccessKeyName=[key-name];SharedAccessKey=[key-value]
                options.ConnectionString = "Endpoint=sb://my-eventhub-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=your-access-key-here";
                options.EventHubName = "my-eventhub";

                // Optional: Configure client options
                // options.ClientOptions = new EventHubProducerClientOptions
                // {
                //     ConnectionOptions = new EventHubConnectionOptions
                //     {
                //         TransportType = EventHubsTransportType.AmqpWebSockets
                //     }
                // };
            })
            .Build();

        await pgOutput2Json.StartAsync(stoppingToken);
    }
}
```

> **Note:** This example uses a **temporary replication slot**, meaning it won't capture changes made while the worker was stopped.
