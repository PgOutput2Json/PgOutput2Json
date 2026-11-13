---
title: Amazon Kinesis
description: Publish PostgreSQL changes to Amazon Kinesis with PgOutput2Json.Kinesis. Delivery is at-least-once.
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

## Delivery and deduplication

The Kinesis adapter delivers at-least-once, like the other message outputs: after a failure or a restart, the changes PostgreSQL replays are sent to Kinesis again.

Consumers of the Kinesis stream must therefore be prepared for duplicated messages and filter them out, using the deduplication key carried by every message:

- `w` — the transaction final LSN of the change event
- `n` — the message number within the transaction

A `(w, n)` pair uniquely identifies a change event. A consumer that remembers the highest pair it processed, per table and shard, can skip anything at or below it.
