---
title: Amazon Kinesis
description: Publish PostgreSQL changes to Amazon Kinesis with PgOutput2Json.Kinesis. The adapter has no deduplication.
---

PgOutput2Json supports pushing row changes as JSON messages to Amazon Kinesis.

> **Important:** First set up PostgreSQL as described in [Getting started](../../getting-started/).

## Create a .NET Worker Service

Set up a new **.NET Worker Service** and add the following package reference:

```
dotnet add package PgOutput2Json.Kinesis
```

In your `Worker.cs`, use the following code to configure change propagation to Amazon Kinesis:

```csharp
using PgOutput2Json;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;

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
            .UseKinesis(options =>
            {
                // assumes LocalStack, with test_stream created in eu-central-1
                options.StreamName = "test_stream";
                options.KinesisConfig.ServiceURL = "http://localhost:4566";
                options.KinesisConfig.UseHttp = true;
                options.KinesisConfig.DefaultAWSCredentials = new BasicAWSCredentials("dummy", "dummy");
                options.KinesisConfig.AuthenticationRegion = "eu-central-1";
            })
            .Build();

        await pgOutput2Json.StartAsync(stoppingToken);
    }
}
```

## Deduplication

The Kinesis adapter **does not support deduplication**. Amazon Kinesis provides no cheap way to read the last published record of a shard (the `LATEST` shard iterator starts *just after* the most recent record), and shards cannot be targeted client-side — so the adapter cannot recover the last published WAL position after a restart. When the worker restarts, PostgreSQL replays the WAL from the slot's confirmed position, and **already-published messages are sent to Kinesis again**.

Consumers of the Kinesis stream must therefore be prepared for duplicated messages and protect themselves from their ill effects by filtering them out, using the deduplication key carried by every message:

- `w` — the transaction final LSN of the change event
- `n` — the message number within the transaction

A `(w, n)` pair uniquely identifies a change event: any event at or below the highest `(w, n)` pair a consumer has already processed was already handled and can be safely skipped.
