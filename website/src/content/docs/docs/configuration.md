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
| `WithReconnectDelay(delay)` | 10 seconds | How long the listener waits after an error (the output is down, the slot is in use, PostgreSQL is not reachable) before it connects again and publishes the unconfirmed changes again. A failed initial sync of a table waits the same time. |

## Initial data copy

| Method | What it does |
|---|---|
| `WithInitialDataCopy(copyData, maxParallelJobs = 2)` | Publishes the existing rows of the published tables. Each table is copied once per replication slot. `maxParallelJobs` is the number of tables synced at the same time. |

The copy works like the table synchronization of PostgreSQL's own logical replication:

1. Streaming starts right away. The changes of a table that is not yet synced are left to its sync worker.
2. A sync worker creates a replication slot for the table and copies the rows from that slot's snapshot. The rows are published with change type `"C"`.
3. The worker then publishes the changes made to the table since the snapshot, until it has caught up with the main stream.
4. The main stream stops between two transactions, the worker publishes the table's changes up to that point, and from there the table belongs to the main stream. The stop is short, because the worker has caught up before.

So nothing is lost and nothing is published twice between the copy and the stream, and a table is always published in order: copied rows, then its changes.

What to know:

- **Copied rows.** They have `"c":"C"` and no position (`"w"` and `"n"` are 0). Upsert them by key, and leave them out of [deduplication](../deduplication/).
- **Other tables are ahead.** While a table is synced, its changes are published later than the changes of the tables that are already streamed. Consumers that keep a deduplication watermark must keep it per table.
- **A failed copy starts again from zero**, and its rows are published again. The copy only adds and replaces rows: a row published by the failed attempt and deleted before the second one stays at the output.
- **A table published while the listener runs** is synced without a restart. The listener learns about it from its first change.
- **Row filters and column lists** of the publications apply to the copy. A table in several publications is copied once; a row is copied if the row filter of any of those publications accepts it.
- **Resources.** Each table being synced uses one replication slot and two connections, on top of the main stream. Size `max_replication_slots` and `max_wal_senders` for that. A long copy keeps its snapshot open, which holds back vacuum.
- **Sync slots.** They are named `<slot>_sync_<table oid>` and are dropped when the table is synced. Slots left behind by a crash are dropped on the next start. If you drop the main slot for good, drop its sync slots too.

### Table states

With a permanent replication slot, the library keeps one row per published table and slot in the source database, in `pgoutput2json.table_sync_state`:

| State | Meaning |
|---|---|
| `init` | The table needs its copy. |
| `copying` | The copy is running. After a restart it starts again from zero. |
| `copied` | The copy is done and the sync slot holds the position. After a restart the sync continues from there, without a new copy. |
| `ready` | The table is streamed by the main stream. |

- The states are written even when the copy is off: every published table is registered as `ready`. If you enable the copy later, only the tables published since then are copied.
- If you turn the copy off while tables are not yet `ready`, they are streamed without a copy, and copied if you turn it on again.
- A table removed from the publications is forgotten on the next start, and so is a table that was dropped and created again. Either is copied again if it comes back.
- To copy a table again, delete its row and restart.
- The states know nothing about the output. If you empty the output, or drop the slot and create it again under the same name, delete the slot's rows too.

The schema and the table are created on first use, which needs the `CREATE` privilege on the database. Alternatively, create the `pgoutput2json` schema yourself and grant the replication user `CREATE` and `USAGE` on it. If the states cannot be stored and the copy is off, the library logs a warning and streams as usual. With the copy on, it is an error.

With a temporary slot nothing is stored, the sync slots are temporary too, and every table is copied on every start and every reconnect.

### Changes from earlier versions

- `WithPgOrderedKeyColumns` and the `copyDataBatchSize` parameter are gone: a copy can no longer be resumed part-way.
- Copied rows have change type `"C"` instead of `"I"`.
- Streaming no longer waits for the copy to finish.
- Tables that the old version had copied are not copied again. The old table `pgoutput2json.data_copy_progress` is read once per slot and can be dropped when all your listeners have been upgraded and started.

## JSON options

Set with `WithJsonOptions(options => ...)`.

| Option | Default | What it does |
|---|---|---|
| `WriteMode` | `Default` | `Compact` writes rows as arrays without column names. See [Compact mode](../change-events/#compact-mode). The outputs that store the rows (SQLite, MongoDB, DynamoDB, ClickHouse) ignore it. |
| `WriteTableNames` | `false` | Adds the schema-qualified table name as `"t"`. |
| `WriteNulls` | `false` | Writes columns with a `null` value. They are left out otherwise. |
| `WriteTimestamps` | `false` | Adds the commit timestamp as `"cts"` and the message timestamp as `"mts"`. |
| `TimestampFormat` | `DateTimeTicks` | The format of `"cts"` and `"mts"`: .NET ticks, or `UnixTimeMilliseconds`. |

## Logging

`WithLoggerFactory(loggerFactory)` sets the logger for the library and for Npgsql.
