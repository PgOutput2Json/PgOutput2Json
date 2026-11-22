---
title: Amazon DynamoDB
description: Copy changed PostgreSQL rows to Amazon DynamoDB tables with PgOutput2Json.DynamoDb.
---

PgOutput2Json supports copying modified PostgreSQL rows to Amazon DynamoDB tables. By default, rows are copied only when they change, using logical replication.
Optionally, initial data copy can be enabled with `WithInitialDataCopy(true)` when configuring the builder.

The PgOutput2Json library will create DynamoDB tables for each table included in logical replication.

A table of the `public` schema gives its name to its DynamoDB table (`orders`), a table of another schema is named `schema.table` (`sales.orders`). Set `options.GetTableName` to choose the names yourself: it gets the schema and the table name apart. Two PostgreSQL tables with the same DynamoDB name are an error. `TableBillingModes` is by the PostgreSQL table (`schema.table`). Versions before 2.2 named every table `schema.table`, also `public.orders`: after the upgrade the rows of a table of the `public` schema go into a table with the new name, which starts empty. To keep the old names, set `options.GetTableName = (schema, table) => $"{schema}.{table}";`.

Tables are created the first time a row belonging to the respective table is changed.
The first PK column is used as partition key. The other PK columns are joined with `|` into a string, stored in the sort key attribute `SK`. An array column is stored as a string with its JSON text.

> **Important:** First set up PostgreSQL as described in [Getting started](../../getting-started/).

## Batch size

Every batch of changes is written with `BatchWriteItem` requests of up to 25 items, and `MaxParallelBatchWrites` of them (8 by default) are in flight at a time. Two changes of the same row are never in flight together, so they are applied in order. The position is saved once per batch, after its rows. `UseDynamoDb` sets the batch size to 1,000 changes (the default of the library is 100), so that the parallel requests have work. In our [benchmark](https://github.com/PgOutput2Json/PgOutput2Json/tree/main/src/PgOutput2Json.Benchmark), against DynamoDB Local, 8 parallel requests were about 2.5 times faster than one (measured on a Hetzner CX33, a cost-optimized cloud server with 4 shared vCPUs and 8 GB RAM, with PostgreSQL and DynamoDB Local in Docker on the same machine). DynamoDB Local is not DynamoDB: on AWS the throughput depends on the network and on the capacity of the tables, so measure it with your own tables (`--dynamodb URL`). To use another size, call `WithBatchSize` after `UseDynamoDb`. A batch that is not full is still written after the batch wait time (100 ms by default).

## Create a .NET Worker Service

Set up a new **.NET Worker Service** and add the following package reference:

```
dotnet add package PgOutput2Json.DynamoDb
```

In your `Worker.cs`, use the following code to configure change propagation to Amazon DynamoDB:

```csharp
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;
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
            .UseDynamoDb(options =>
            {
                options.ClientConfig = new AmazonDynamoDBConfig
                {
                    ServiceURL = "http://localhost:8000",                              // for local DynamoDB, change/remove for AWS
                    UseHttp = true,                                                    // for local DynamoDb, remove for AWS
                    DefaultAWSCredentials = new BasicAWSCredentials("dummy", "dummy"), // for local DynamoDB, remove for AWS
                    MaxErrorRetry = 3,                                                 // retry failed requests up to 3 times
                };
            })
            .Build();

        await pgOutput2Json.StartAsync(stoppingToken);
    }
}
```
