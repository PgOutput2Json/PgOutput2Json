---
title: Redis
description: Publish PostgreSQL changes to Redis streams or Pub/Sub channels with PgOutput2Json.Redis.
---

PgOutput2Json supports publishing JSON messages to Redis streams (default) or Pub/Sub channels.

> **Important:** First set up PostgreSQL as described in [Getting started](../../getting-started/).

## Set Up Redis

Make sure you have a Redis instance running and accessible to your .NET application. This example assumes Redis is running locally on the default port `6379`.

## Create .NET Worker Service

Set up a new **.NET Worker Service** and add the following package reference:

```
dotnet add package PgOutput2Json.Redis
```

In your `Worker.cs`, use the following code to publish change events to Redis:

```csharp
using PgOutput2Json;
using PgOutput2Json.Redis;

public class Worker : BackgroundService
{
    private readonly ILoggerFactory _loggerFactory;

    public Worker(ILoggerFactory loggerFactory)
    {
        _loggerFactory = loggerFactory;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // This code assumes PostgreSQL and Redis are on localhost
        using var pgOutput2Json = PgOutput2JsonBuilder.Create()
            .WithLoggerFactory(_loggerFactory)
            .WithPgConnectionString("server=localhost;database=my_database;username=pgoutput2json;password=_your_password_here_")
            .WithPgPublications("my_publication")
            .UseRedis(options =>
            {
                options.StreamName = "my_stream";
                options.PublishMode = PublishMode.Stream;
                options.StreamNameSuffix = StreamNameSuffix.None; // or TableName, or TableNameAndPartition
                options.Redis.EndPoints.Add("localhost:6379");
            })
            .Build();

        await pgOutput2Json.StartAsync(stoppingToken);
    }
}
```

JSON messages will be published to the specified Redis stream. If a stream name suffix is specified, the stream or channel name becomes dynamic, using the format: `stream_name:schema.table:partition`.

> **Note:** The table name is always qualified with the schema using the `.` character.
