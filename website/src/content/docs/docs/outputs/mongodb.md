---
title: MongoDB
description: Copy changed PostgreSQL rows to MongoDB collections with PgOutput2Json.MongoDb.
---

PgOutput2Json supports copying modified PostgreSQL rows to MongoDB collections. By default, rows are copied only when they change, using logical replication.
Optionally, initial data copy can be enabled with `WithInitialDataCopy(true)` when configuring the builder.

The PgOutput2Json library will create the MongoDB database if it does not already exist, along with one collection for each table included in logical replication. Collections are created the first time a row belonging to the respective table is changed. Additionally, one unique index is created for each collection, covering the fields that are a part of the primary key.

> **Important:** First set up PostgreSQL as described in [Getting started](../../getting-started/).

## Create a .NET Worker Service

Set up a new **.NET Worker Service** and add the following package reference:

```
dotnet add package PgOutput2Json.MongoDb
```

In your `Worker.cs`, use the following code to configure change propagation to MongoDB:

```csharp
using PgOutput2Json;
using MongoDB.Driver;
using MongoDB.Driver.Core.Configuration;

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
            .UseMongoDb(options =>
            {
                options.DatabaseName = "my_mongo_database";
                options.ClientSettings = new MongoClientSettings
                {
                    Server = new MongoServerAddress("localhost", 27017),
                    Scheme = ConnectionStringScheme.MongoDB,
                    Credential = MongoCredential.CreateCredential("admin", "admin", "__your_mongo_password_here__"),
                };
            })
            .Build();

        await pgOutput2Json.StartAsync(stoppingToken);
    }
}
```
