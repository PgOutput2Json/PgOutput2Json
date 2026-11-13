---
title: Webhooks
description: Deliver PostgreSQL changes in batches to an HTTP endpoint with PgOutput2Json.Webhooks.
---

PgOutput2Json supports delivering row changes in batches to an HTTP endpoint.

> **Important:** First set up PostgreSQL as described in [Getting started](../../getting-started/).

## Create a .NET Worker Service

Set up a new **.NET Worker Service** and add the following package reference:

```
dotnet add package PgOutput2Json.Webhooks
```

In your `Worker.cs`, use the following code to configure change propagation to a webhook:

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
            .UseWebhook("https://example.com/webhooks/pghook", options =>
            {
                // Optional: Configure Webhook options
                // options.WebhookSecret = "test";
                // options.ConnectTimeout = TimeSpan.FromSeconds(10);
                // options.RequestTimeout = TimeSpan.FromSeconds(30);
            })
            .Build();

        await pgOutput2Json.StartAsync(stoppingToken);
    }
}
```

## Webhook payload

Changes are delivered in batches to your webhook. The POST request body is an array of these elements:

```jsonc
{
  "c": "U",             // Change type: I (insert), U (update), D (delete)
  "w": 2485645760,      // Transaction final LSN — part of the deduplication key
  "n": 1,               // Message number within the transaction — part of the deduplication key
  "t": "schema.table",  // Table name (if enabled in JSON options)
  "k": { ... },         // Key values — included for deletes, and for updates if the key changed,
                        // or old row values, if the table uses REPLICA IDENTITY FULL
  "r": { ... }          // New row values (not present for deletes)
}
```

## Webhook metadata

If `options.UseStandardWebhooks` is `false`, which is the default, each request includes:
- `X-Timestamp`: Unix timestamp of when the payload was signed
- `X-Hub-Signature-256`: HMAC-SHA256 of the request body using your secret (GitHub style, optional, only sent if `options.WebhookSecret` is set)
  See: [Validating Webhook Deliveries](https://docs.github.com/en/webhooks/using-webhooks/validating-webhook-deliveries)

If `options.UseStandardWebhooks` is `true` then each request includes standard headers:
- `webhook-id`: Id of the message in format FirstDedupKey_LastDedupKey, (eg. `2485645760_2485645760`).
  Note that this is not fully standard compliant. It can only be used for idempotency check if the `BatchSize` is 1.
  Otherwise, deduplication keys from the individual messages should be used.
- `webhook-timestamp`: Integer unix timestamp (seconds since epoch).
- `webhook-signature`: The signature of the webhook. See: [Standard Webhooks](https://www.standardwebhooks.com/).

In both cases, the request includes:
- `User-Agent`: string in this format: `PgHook/ReplicationSlotName`;
