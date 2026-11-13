---
title: Configuration
description: The PgOutput2JsonBuilder methods and the JSON options, with their defaults.
---

Everything is configured through `PgOutput2JsonBuilder`. `Build()` throws an `InvalidOperationException` if the connection, the publications or the output are missing.

```csharp
using var pgOutput2Json = PgOutput2JsonBuilder.Create()
    .WithLoggerFactory(loggerFactory)
    .WithPgConnectionString("server=localhost;database=my_database;username=pgoutput2json;password=_your_password_here_")
    .WithPgPublications("my_publication")
    .WithPgReplicationSlot("my_slot")
    .WithBatchSize(100)
    .WithJsonOptions(options => options.WriteTableNames = true)
    .WithMessageHandler((json, table, key, partition) => Task.FromResult(true))
    .Build();
```

## PostgreSQL

| Method | What it does |
|---|---|
| `WithPgConnectionString(connectionString)` | The connection string. The user needs the `REPLICATION` attribute. Required, unless `WithPgDataSource` sets it. |
| `WithPgDataSource(builder => ...)` | Configures the `NpgsqlDataSourceBuilder` directly. |
| `WithPgPublications(names)` | The publications to read. At least one is required. |
| `WithPgReplicationSlot(name, useTemporarySlot = false)` | The replication slot. Without this call a temporary slot is used, and the changes made while the worker is stopped are lost. See [Permanent replication slots](../replication-slots/). |
| `WithPgColumns(table, columns)` | The columns of a table to include in the JSON. All columns are included for a table that is not listed. |

Table names are schema-qualified, for example `public.my_table`.

## Output

| Method | What it does |
|---|---|
| `WithMessageHandler(handler)` | Handles each change in your application. The handler receives the JSON, the table name, the message key and the partition key. |
| `WithMessagePublisherFactory(factory)` | A custom output. The adapter packages call this from their `Use...` methods, for example `UseKafka`. |
| `WithPartitionKeyFields(table, field, additionalFields)` | The columns whose values partitioned outputs route by. Use columns that are part of the table's replica identity, or deletes carry no partition key. See [Change events](../change-events/#message-key). |

## Delivery

| Method | Default | What it does |
|---|---|---|
| `WithBatchSize(batchSize)` | `100` | The number of messages published before they are confirmed to the output and to PostgreSQL. A smaller batch is confirmed after 100 ms without new messages. |
| `WithDeduplication(useDeduplication)` | `true` | SQLite, MongoDB and DynamoDB only: skips the changes PostgreSQL replays after a reconnect. See [Delivery and deduplication](../deduplication/). |
| `WithIdleWalMessageInterval(interval)` | 10 seconds | How often a small message is written to the WAL while the published tables are idle, so the slot keeps moving when only other tables change. `TimeSpan.Zero` disables it. |

## Initial data copy

| Method | What it does |
|---|---|
| `WithInitialDataCopy(copyData, maxParallelJobs = 1, copyDataBatchSize = 0)` | Publishes the existing rows before streaming starts. The output must be able to report its last published position: the copy starts only when nothing was published yet. |
| `WithPgOrderedKeyColumns(table, columns)` | The columns the copy of a table is ordered by. They are needed to copy in batches (`copyDataBatchSize`) and to resume an interrupted copy. |

## JSON options

Set with `WithJsonOptions(options => ...)`.

| Option | Default | What it does |
|---|---|---|
| `WriteMode` | `Default` | `Compact` writes rows as arrays without column names. See [Compact mode](../change-events/#compact-mode). |
| `WriteTableNames` | `false` | Adds the schema-qualified table name as `"t"`. |
| `WriteNulls` | `false` | Writes columns with a `null` value. They are left out otherwise. |
| `WriteTimestamps` | `false` | Adds the commit timestamp as `"cts"` and the message timestamp as `"mts"`. |
| `TimestampFormat` | `DateTimeTicks` | The format of `"cts"` and `"mts"`: .NET ticks, or `UnixTimeMilliseconds`. |

## Logging

`WithLoggerFactory(loggerFactory)` sets the logger for the library and for Npgsql.
