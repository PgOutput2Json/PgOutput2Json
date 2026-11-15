---
title: RabbitMQ
description: Publish PostgreSQL changes to a RabbitMQ topic exchange with PgOutput2Json.RabbitMq.
---

This document assumes you have a running RabbitMQ instance working on the default port, with the default `guest`/`guest` user allowed from the localhost.

> **Important:** First set up PostgreSQL as described in [Getting started](../../getting-started/).

## Create Topic Exchange

In RabbitMQ, create a `durable` `topic` type `exchange` where PgOutput2Json will be pushing JSON messages. We will assume the name of the exchange is `my_exchange` in the `/` virtual host.

## Create and Bind a Queue to Hold the JSON Messages

Create a `durable` queue named `my_queue` and bind it to the exchange created in the previous step. Use `public.#` as the routing key. This ensures that it will receive JSON messages for all tables in the `public` schema.

> **Note:** Use a different schema name if your tables are in a different schema.

## Create a .NET Worker Service

Set up a new **.NET Worker Service** and add the following package reference:

```
dotnet add package PgOutput2Json.RabbitMq
```

In your `Worker.cs`, add the following code:

```csharp
using PgOutput2Json;
using PgOutput2Json.RabbitMq;

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
            .UseRabbitMq(options =>
            {
                options.HostNames = [ "localhost" ];
                options.Username = "guest";
                options.Password = "guest";
                options.VirtualHost = "/";
                options.ExchangeName = "my_exchange";

                // set to true for persistent messages
                options.UsePersistentMessagesByDefault = false;
            })
            .Build();

        await pgOutput2Json.StartAsync(stoppingToken);
    }
}
```

Run the code, and with a little luck, you should see JSON messages being pushed into the `my_queue` in RabbitMQ when you make a change in the tables specified in `my_publication`.
The routing key will be in the form: `schema.table.key_partition`. Since we did not configure anything specific in the PgOutput2Json, the `key_partition` will always be `0`.

## Batch size

At the end of every batch the listener waits for the confirms of RabbitMQ (publisher confirms). `UseRabbitMq` therefore sets the batch size to 1,000 changes (the default of the library is 100). In our [benchmark](https://github.com/PgOutput2Json/PgOutput2Json/tree/main/src/PgOutput2Json.Benchmark), with a durable queue bound to the exchange, the initial copy and the stream ran at about 31,000 rows per second with batches of 1,000, and at about 24,000 and 21,000 with batches of 100 (measured on a Hetzner CX33, a cost-optimized cloud server with 4 shared vCPUs and 8 GB RAM, with PostgreSQL and RabbitMQ in Docker on the same machine). Larger batches added nothing: RabbitMQ itself used most of the CPU, routing and confirming every message, also when the messages were not persistent. For more throughput, use [RabbitMQ Streams](../rabbitmq-streams/). To use another size, call `WithBatchSize` after `UseRabbitMq`. A batch that is not full is still confirmed after the batch wait time (100 ms by default).
