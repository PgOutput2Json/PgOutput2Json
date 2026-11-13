---
title: RabbitMQ Streams
description: Publish PostgreSQL changes to RabbitMQ streams and super streams with PgOutput2Json.RabbitMqStreams.
---

Using RabbitMQ Streams is similar to the standard RabbitMQ setup, but instead of creating an exchange and binding queues, you need to create a stream, and configure the stream protocol in RabbitMQ.

> **Important:** First set up PostgreSQL as described in [Getting started](../../getting-started/).

## Set Up RabbitMQ Streams

Before using RabbitMQ Streams, ensure that the RabbitMQ Streams plugin is enabled. The stream protocol must be active for the stream to work.

Create a stream in RabbitMQ, for example, a stream named `my_stream`. You can do this from the RabbitMQ management interface or through the appropriate RabbitMQ commands.

## Create .NET Worker Service

Set up a new **.NET Worker Service** and add the following package reference:

```
dotnet add package PgOutput2Json.RabbitMqStreams
```

In your `Worker.cs`, add the following code to use RabbitMQ Streams:

```csharp
using PgOutput2Json;
using PgOutput2Json.RabbitMqStreams;

public class Worker : BackgroundService
{
    private readonly ILoggerFactory _loggerFactory;

    public Worker(ILoggerFactory loggerFactory)
    {
        _loggerFactory = loggerFactory;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // This code assumes PostgreSQL and RabbitMQ are on localhost
        using var pgOutput2Json = PgOutput2JsonBuilder.Create()
            .WithLoggerFactory(_loggerFactory)
            .WithPgConnectionString("server=localhost;database=my_database;username=pgoutput2json;password=_your_password_here_")
            .WithPgPublications("my_publication")
            .UseRabbitMqStreams(options =>
            {
                options.StreamName = "my_stream";
                options.StreamSystemConfig.UserName = "guest";
                options.StreamSystemConfig.Password = "guest";
                options.StreamSystemConfig.VirtualHost = "/";
                options.StreamSystemConfig.Endpoints =
                [
                    new IPEndPoint(IPAddress.Loopback, 5552)
                ];
            })
            .Build();

        await pgOutput2Json.StartAsync(stoppingToken);
    }
}
```

Once the stream is configured in RabbitMQ and the .NET service is running, changes to the PostgreSQL tables specified in `my_publication` will be pushed to the `my_stream` stream in RabbitMQ.
No need to manage exchanges or queues — everything will flow directly through the stream.

## Super Streams and Compact Mode

In compact mode (`JsonWriteMode.Compact`), row values are sent without column names, and the schema (`"s"`) comes only with the first row of a table after a connect or a table change. A super stream spreads the rows of a table across its partitions, so the schema lands in only one partition.

With compact mode on a super stream, consumers must therefore share the last schema per table across all partitions, for example in a common store. A single (non-super) stream has no such limitation.
