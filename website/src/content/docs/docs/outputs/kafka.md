---
title: Kafka
description: Publish PostgreSQL changes to a Kafka topic with PgOutput2Json.Kafka, and its delivery guarantees.
---

Kafka is supported out of the box and works similarly to the RabbitMQ integrations. PgOutput2Json will publish JSON messages directly to a Kafka topic.

> **Important:** First set up PostgreSQL as described in [Getting started](../../getting-started/).

## Set Up Kafka

Ensure you have a running Kafka broker accessible to your .NET application. This example assumes Kafka is running locally on the default port `9092`.

Create a topic in Kafka named `my_topic`. You can do this using Kafka’s command-line tools or through a Kafka UI.

## Create .NET Worker Service

Set up a new **.NET Worker Service** and add the following package reference:

```
dotnet add package PgOutput2Json.Kafka
```

In your `Worker.cs`, use the following code to publish changes to Kafka:

```csharp
using PgOutput2Json;
using PgOutput2Json.Kafka;

public class Worker : BackgroundService
{
    private readonly ILoggerFactory _loggerFactory;

    public Worker(ILoggerFactory loggerFactory)
    {
        _loggerFactory = loggerFactory;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // This code assumes PostgreSQL and Kafka are on localhost
        using var pgOutput2Json = PgOutput2JsonBuilder.Create()
            .WithLoggerFactory(_loggerFactory)
            .WithPgConnectionString("server=localhost;database=my_database;username=pgoutput2json;password=_your_password_here_")
            .WithPgPublications("my_publication")
            .UseKafka(options =>
            {
                options.ProducerConfig.BootstrapServers = "localhost:9092";
                options.Topic = "my_topic";
            })
            .Build();

        await pgOutput2Json.StartAsync(stoppingToken);
    }
}
```

Once the service is running, any changes made to the tables listed in `my_publication` will be published to the `my_topic` topic in Kafka as JSON messages.

## Delivery Guarantees

Messages are confirmed to PostgreSQL in batches, and only after Kafka has acknowledged every message in the batch. If the batch fails, it is not confirmed: the publisher reconnects and PostgreSQL replays the unconfirmed changes.

Delivery is at-least-once: after a failure, the replayed changes are published to Kafka again, and consumers deduplicate on the `w`/`n` fields. To make that simple, every partition receives its messages in order and without gaps: a consumer can remember the highest `(w, n)` pair it processed, per table and partition, and skip anything at or below it. So, unless you set them yourself, the publisher configures the producer with:

| Setting | Value | Why |
|---|---|---|
| `EnableIdempotence` | `true` | The broker rejects out-of-sequence writes, so retries cannot reorder or duplicate messages. |
| `MessageTimeoutMs` | `0` (infinite) | librdkafka times out each message separately. A timed-out message is skipped while later ones are still delivered, which leaves a gap. |
| `EnableGaplessGuarantee` | `true` | Any other error that could leave a gap stops the producer with a fatal error instead. librdkafka marks this setting as experimental. |
| `Partitioner` | `Murmur2Random` | The partitioner of the Java client, so a key lands in the same partition as with other Kafka producers. |

The time limit applies to the whole batch instead, through `KafkaPublisherOptions.ConfirmTimeout` (5 minutes by default). While Kafka is unavailable, the confirm waits and logs a warning every 30 seconds. When the timeout expires, the producer is discarded and the listener reconnects. The whole producer is discarded, never single messages, so no gap can form.

Things to be aware of:

- Idempotence requires `acks=all`, at most 5 in-flight requests and retries above 0. If you set an incompatible value explicitly (for example `Acks = Acks.Leader`), the producer fails to start.
- If you change the first three settings, a failed delivery can leave a gap. The gap is filled when the batch is replayed, but the messages are then out of order, and a consumer that keeps a watermark skips the late ones.
- If a message is permanently rejected (for example, it is larger than the topic's `max.message.bytes`), the publisher keeps failing on that message until the problem is fixed. Skipping it would lose the change.
- While the confirm waits, no further changes are read, and PostgreSQL retains the WAL for the replication slot until the batch is confirmed. A long Kafka outage therefore grows `pg_wal` on the database server.

### Routing

Messages are routed by the message key. With partition key columns configured for a table (`WithPartitionKeyFields`), its rows are routed by those columns instead, with the same hash (murmur2). Messages without a key go to a random partition.
