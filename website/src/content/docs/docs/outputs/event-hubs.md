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

Event Hubs uses the partition key (derived from table name) to distribute messages across partitions. This ensures:

- Messages from the same table are processed in order
- Load is distributed across multiple partitions for better throughput
- Parallel processing capabilities for consumers

With partition-key columns configured for a table (`WithPartitionKeyFields`), its rows are routed by those columns instead, and the partition is chosen client-side (murmur2 over the column values).

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
