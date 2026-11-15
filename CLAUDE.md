# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Build the entire solution
dotnet build src/PgOutput2Json.sln

# Run all tests
dotnet test src/PgOutput2Json.Tests/PgOutput2Json.Tests.csproj

# Run a single test by name
dotnet test src/PgOutput2Json.Tests/PgOutput2Json.Tests.csproj --filter "FullyQualifiedName~WriteText_should_add_quotes"

# Run the integration tests (need Docker - they start PostgreSQL and Kafka with Testcontainers)
dotnet test src/PgOutput2Json.IntegrationTests/PgOutput2Json.IntegrationTests.csproj

# Run the manual integration test worker
dotnet run --project src/PgOutput2Json.TestWorker/PgOutput2Json.TestWorker.csproj

# Run the benchmark (Docker; see src/PgOutput2Json.Benchmark/README.md for the options and profiling)
dotnet run -c Release --project src/PgOutput2Json.Benchmark -- --rows 1000000 --output null,clickhouse
```

The benchmark measures the initial copy and catching up with rows in the WAL, end to end, with a null output (the library alone), ClickHouse, SQLite, RabbitMQ Streams or Kafka. A wrapper around the output's factory (`CountingPublisherFactory`, using the builder's internal `MessagePublisherFactory`) counts the confirmed rows, and it reports rows/s, CPU and allocations per row. On Linux it connects to the containers at their own IP, not through Docker's port proxy.

Both test projects run on every pull request and on `main` (`.github/workflows/tests.yml`). The integration tests use PostgreSQL 16 unless `PG_IMAGE` is set (e.g. `PG_IMAGE=postgres:14`); the CI repeats them on versions 14 to 18. A test that needs a newer version calls `Containers.RequirePg`. The tests of the outputs are in the `Outputs` namespace and start their own containers (Kafka, RabbitMQ, Redis, MongoDB, DynamoDB Local, LocalStack, the Event Hubs emulator, ClickHouse) on first use; they share `MessageScenario` (message outputs) and `StoreScenario` (SQLite, MongoDB, DynamoDB, ClickHouse). The Event Hubs emulator cannot create an event hub while it runs: a test that needs one adds it to `Containers.EventHubs`. Run only them with `--filter "FullyQualifiedName~.Outputs."`, or leave them out with `!~`.

The website (landing page and docs, Astro + Starlight) is in `website/` and is deployed to GitHub Pages by `.github/workflows/website.yml`. Its docs pages were split from `README.md`: when the README or an adapter README changes, update the matching page in `website/src/content/docs/docs/`. See `website/README.md`. Each package README (`src/<Package>/README.md`, shown on NuGet) is built from them: the root README up to its Quick Start, the Quick Start steps 1.1 to 1.3 (for the core package also 1.4), a `## Using <Output>` section with the output's website page (headings one level down, links absolute to https://pg2json.com/docs/, the temporary slot note after the worker example), and the permanent replication slots section. Keep them in sync when any of these change.

The core library targets `net8.0`, `net9.0`, and `net10.0`. `TreatWarningsAsErrors=True` is set — all warnings must be resolved.

## Architecture

The library listens to a PostgreSQL logical replication slot (`pgoutput` plugin) and converts WAL change events into JSON messages, then dispatches them to pluggable publisher backends.

### Core Data Flow

```
PostgreSQL WAL
    → ReplicationListener (connects, reads pgoutput messages)
        → JsonWriter (converts to JsonMessage with JSON, table name, key, partition values)
            → IMessagePublisher (publishes and confirms batches)
```

### Key Components (`src/PgOutput2Json/`)

- **`PgOutput2JsonBuilder`** — Fluent builder. Entry point for users. Validates config and creates `ReplicationListener` + `PgOutput2Json`.
- **`PgOutput2Json` / `IPgOutput2Json`** — Thin wrapper around `ReplicationListener`; manages the `CancellationTokenSource` and exposes `StartAsync`.
- **`ReplicationListener`** — The core loop. Opens a `LogicalReplicationConnection`, starts replication, dispatches each WAL message. Manages two timers: an idle confirmation timer (sends WAL status after `BatchWaitTime`) and an idle WAL message timer (calls `pg_logical_emit_message` to prevent `restart_lsn` from lagging). Auto-reconnects after errors with a 10s delay. Uses `AsyncLock` to coordinate the main loop and timer callbacks. When it starts it builds an `NpgsqlDataSource` from `DataSourceBuilder` and passes it on: every regular connection comes from it (never `new NpgsqlConnection(connectionString)`), so they log through the builder's logger factory. The library never calls `NpgsqlLoggingConfiguration.InitializeLogging` (global, belongs to the application); the replication connections can only be created from a connection string and log through that global configuration.
- **`JsonWriter`** — Converts `InsertMessage`, `UpdateMessage` (Default/Full/Index), and `DeleteMessage` into a `JsonMessage`. Supports `Default` and `Compact` write modes. In Compact mode, column names are omitted from row arrays and sent in a `"s"` schema property with the first row after each Relation message (first use of a relation on a connection, or a change). The schema has the type name of a column whose type is not built in (`AddType`, from the Type messages pgoutput sends before a Relation message; for a domain the name of its base type), and `TableCopy` reads the same name from `pg_type`, so the copy and the stream send the same schema.
- **`JsonMessage`** — Reused single instance per `JsonWriter`. Holds `StringBuilder`s for: JSON body, table name, key column value(s), and partition key value(s).
- **`IMessagePublisher` / `MessagePublisher`** — Interface for all output backends. Must implement `PublishAsync` and `ConfirmAsync`. `GetLastPublishedWalSeqAsync` is optional (default `(0,0)`): only the outputs that save their position with the data implement it, and the listener then skips replayed messages at or below that position.
- **`IMessagePublisherFactory`** — Creates a new `IMessagePublisher` per connection/slot. All adapter packages implement this. `RequiredWriteMode` (default null) is the write mode the output needs: the stores return `Compact`, and `Build()` sets it whatever `WithJsonOptions` said. The message outputs leave the write mode to the user (default: named objects).
- **`TableSyncState`** — Keeps one row per published table and replication slot in the source database (`pgoutput2json.table_sync_state`): `init`, `copying`, `copied` or `ready`, with an LSN. Written on every connect, even with the copy off, so enabling the copy later copies only new tables. Nothing is stored for a temporary slot.
- **`TableSync`** — The initial sync, modelled on PostgreSQL's own table synchronization. Created per connection of the main stream. Runs up to `MaxParallelCopyJobs` workers; each one creates a sync slot (`<slot>_sync_<oid>`) with an exported snapshot, copies the table (`TableCopy`, rows with `"c":"C"`), streams the table's changes from the sync slot until it has caught up, then requests a handover. The main loop skips the tables that are not `ready` (`ShouldPublish`), and between transactions (`AtBoundary` / `HandOverAsync`) confirms its batch, gives the worker its position and waits until the worker has published up to it. The main loop only reads memory; the workers use their own connections and publisher instances. Their messages are marked `JsonMessage.IsTableSync`, and the stores do not save a position for them.
- **`TableCopy`** — Copies one table with `COPY (SELECT ... FROM ONLY ...) TO STDOUT` inside the sync slot's snapshot, applying the publication's row filter and column list. It copies the table (or each leaf partition of a partitioned table) in ctid ranges of `PagesPerChunk` pages (TID range scans), and `CopyProgress` holds the chunks the publisher confirmed. With `CopyWorkersPerTable` above 1 (`WithInitialDataCopy(copyData, maxParallelJobs, workersPerTable)`), the workers take chunks from a shared queue; the first one uses the sync worker's connection and publisher, each other one its own connection, importing the same snapshot, and its own publisher. An output whose factory says `SupportsParallelCopy => false` (SQLite) gets one worker. When one worker fails, the others are cancelled. On an output error the sync worker keeps its replication connection, so the exported snapshot stays valid, and calls the copy again with a new publisher, for the chunks that were not confirmed; when the snapshot cannot be imported (`SnapshotUnavailableException`) the sync starts again from zero.

### Publisher Adapter Packages

Each adapter in `src/PgOutput2Json.<Name>/` provides:
- An `IMessagePublisherFactory` implementation
- An extension method on `PgOutput2JsonBuilder` (e.g., `UseRabbitMq(...)`, `UseKafka(...)`)

Available adapters: `RabbitMq`, `RabbitMqStreams`, `Kafka`, `Redis`, `Sqlite`, `MongoDb`, `Kinesis`, `DynamoDb`, `ClickHouse`, `AzureEventHubs`, `Webhooks`.

### Delivery and Deduplication

Delivery is at-least-once: a batch is confirmed to PostgreSQL only after the output acknowledged it, and PostgreSQL replays unconfirmed changes after a reconnect. Each published message carries a deduplication key: the transaction final LSN ("w" in the JSON) plus a counter ("n") that is reset at every `BeginMessage`. Consumers deduplicate with it.

The message outputs (Kafka, RabbitMQ, RabbitMQ Streams, Redis, Kinesis, Event Hubs, Webhooks) publish replayed changes again. They must keep the messages of a partition in WAL order and never skip a failed message, so a consumer can keep a "highest position seen" watermark per table and partition. Known exception: Kafka can accept messages sent after a rejected one, and the replay then fills the gap out of order; `EnableGaplessGuarantee` (experimental in librdkafka, off by default) prevents it.

The stores (SQLite, MongoDB, DynamoDB) save the last position with the data, as one value `w/n` under the config key `Position` (written after the rows, so that it is never ahead of them; the keys `WalEnd` and `MessageNo` of earlier versions are only read), and return it from `GetLastPublishedWalSeqAsync`; the listener skips replayed messages at or below it (`ReplicationListenerOptions.UseDeduplication`, on by default). `UseSqlite` sets the batch size to 1,000 (one transaction per batch). SQLite writes through a connection of its own (`Pooling = false`, so pooled connection settings such as `query_only` neither reach it nor leave it) with `PRAGMA foreign_keys = OFF`, like `session_replication_role = replica` in PostgreSQL's own replication. MongoDB, like ClickHouse, turns the PostgreSQL literal of an array of an enum or user type (type name starting with `_` in the schema) into an array of strings; an array in a key column is its JSON text (the unique index would index each element).

ClickHouse is a store that saves no position. It only inserts: every change is a new row with `_version` (`w * 2^64 + n`), `_deleted` and `_synced_at` (filled in by ClickHouse, only in tables it creates), in a `ReplacingMergeTree` ordered by the key, so a replayed change is the same row again. `CleanUpDeletedRowsAfter` (off by default) sets the experimental cleanup merge settings, so ClickHouse removes deleted rows once a table has had no new rows for that long. `Tables` sets the ORDER BY (exactly the key columns, reordered), PARTITION BY (key columns only) and engine (`ReplacingMergeTree`, `MergeTree` for history, `Null`) per table, used only when the table is created; the key-only rule is because ClickHouse replaces rows by the ORDER BY within a partition and delete rows carry only the key. It uses the HTTP interface (no client library) and sends the rows as `JSONCompactEachRow`, or with `UseRowBinary` (off by default, like binary in PostgreSQL's replication: ClickHouse's own parser decides what JSON values become, while RowBinary re-implements its text rules) in `RowBinary`. RowBinary has no conversion, so every table's column types are read from `system.columns`, parsed into `ClickHouseBinaryType` (the types it creates, any width/Nullable/precision/time zone, all integers to 256 bits, Date, DateTime, Enum8/16, FixedString, IPv4/6, LowCardinality), and `RowBinaryValues` writes each value as that type (dates clamped to the server's range, `timestamp` in the column's or the server's time zone, both read once per connection); a table with another type gets `JSONCompactEachRow`, which ClickHouse converts. Asynchronous inserts (`UseAsyncInserts`) are used only for batches under 1,000 rows, since one insert at a time would just wait for the flush. The integration tests run once more with `CLICKHOUSE_ROWBINARY=true` (RowBinary). It reads an unchanged TOAST value back from ClickHouse, stores json in the `JSON` type (values that are not objects as `{"v": value}`), stores arrays in `Array(Nullable(T))` (`UseArrayTypes`, not for key columns; arrays of enums and user types, whose value is the PostgreSQL literal, are `Array(Nullable(String))`, recognized by the `_` of their type name in the schema; one dimension, because PostgreSQL does not send it, and a multi-dimensional value is an error unless the real ClickHouse column, read from `system.columns`, is not an array), and `UseClickHouse` raises the batch size to 10,000.

`UseRabbitMqStreams` sets the batch size to 10,000 (the producer's MaxInFlight is the batch size); a plain stream gets the messages in lists of MessageBufferSize (`Producer.Send(List<Message>)`, one frame each), a super stream one by one, and the producer's `IsOpen` is asked once a batch. `UseKafka` sets the batch size to 10,000 (the confirm waits for Kafka to acknowledge the whole batch), and the producer takes `byte[]` made from the StringBuilders (`Utf8Buffer.ToArray`). Routing: Kafka routes by message key with librdkafka's `Murmur2Random` partitioner; with partition-key columns the publisher picks the partition itself with the same hash (`MurmurHash2` in the core project, also used by Event Hubs and Redis). RabbitMQ super streams are routed by the client library.

### Batching

Messages are not confirmed to PostgreSQL individually. Instead, `ReplicationListener` collects up to `BatchSize` messages and calls `ConfirmAsync` (which also calls `Connection.SendStatusUpdate`). If fewer than `BatchSize` messages arrive, the idle confirmation timer fires after `BatchWaitTime` (minimum 10ms, default 100ms).

### `JsonOptions` vs `ReplicationListenerOptions`

- **`JsonOptions`** — Controls what goes in the JSON: nulls, timestamps, table names, write mode (Default/Compact), timestamp format.
- **`ReplicationListenerOptions`** — Controls the listener behavior: slot name, publication names, batch size, partition key columns per table, columns to include per table, initial data copy settings, idle WAL message interval.
