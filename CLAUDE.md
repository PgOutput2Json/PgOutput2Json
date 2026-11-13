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
```

The website (landing page and docs, Astro + Starlight) is in `website/` and is deployed to GitHub Pages by `.github/workflows/website.yml`. Its docs pages were split from `README.md`: when the README or an adapter README changes, update the matching page in `website/src/content/docs/docs/`. See `website/README.md`.

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
- **`ReplicationListener`** — The core loop. Opens a `LogicalReplicationConnection`, starts replication, dispatches each WAL message. Manages two timers: an idle confirmation timer (sends WAL status after `BatchWaitTime`) and an idle WAL message timer (calls `pg_logical_emit_message` to prevent `restart_lsn` from lagging). Auto-reconnects after errors with a 10s delay. Uses `AsyncLock` to coordinate the main loop and timer callbacks.
- **`JsonWriter`** — Converts `InsertMessage`, `UpdateMessage` (Default/Full/Index), and `DeleteMessage` into a `JsonMessage`. Supports `Default` and `Compact` write modes. In Compact mode, column names are omitted from row arrays and sent in a `"s"` schema property with the first row after each Relation message (first use of a relation on a connection, or a change).
- **`JsonMessage`** — Reused single instance per `JsonWriter`. Holds `StringBuilder`s for: JSON body, table name, key column value(s), and partition key value(s).
- **`IMessagePublisher` / `MessagePublisher`** — Interface for all output backends. Must implement `PublishAsync` and `ConfirmAsync`. `GetLastPublishedWalSeqAsync` is optional (default `(0,0)`): only the outputs that save their position with the data implement it, and the listener then skips replayed messages at or below that position.
- **`IMessagePublisherFactory`** — Creates a new `IMessagePublisher` per connection/slot. All adapter packages implement this.
- **`DataExporter`** — Handles optional initial data copy via PostgreSQL `COPY ... TO STDOUT HEADER`. Tracks progress in a `__pg2j_data_copy_progress` table. Runs before replication starts.

### Publisher Adapter Packages

Each adapter in `src/PgOutput2Json.<Name>/` provides:
- An `IMessagePublisherFactory` implementation
- An extension method on `PgOutput2JsonBuilder` (e.g., `UseRabbitMq(...)`, `UseKafka(...)`)

Available adapters: `RabbitMq`, `RabbitMqStreams`, `Kafka`, `Redis`, `Sqlite`, `MongoDb`, `Kinesis`, `DynamoDb`, `AzureEventHubs`, `Webhooks`.

### Delivery and Deduplication

Delivery is at-least-once: a batch is confirmed to PostgreSQL only after the output acknowledged it, and PostgreSQL replays unconfirmed changes after a reconnect. Each published message carries a deduplication key: the transaction final LSN ("w" in the JSON) plus a counter ("n") that is reset at every `BeginMessage`. Consumers deduplicate with it.

The message outputs (Kafka, RabbitMQ, RabbitMQ Streams, Redis, Kinesis, Event Hubs, Webhooks) publish replayed changes again. They must keep the messages of a partition in WAL order and never skip a failed message, so a consumer can keep a "highest position seen" watermark per table and partition. Known exception: Kafka can accept messages sent after a rejected one, and the replay then fills the gap out of order; `EnableGaplessGuarantee` (experimental in librdkafka, off by default) prevents it.

The stores (SQLite, MongoDB, DynamoDB) save the last position with the data and return it from `GetLastPublishedWalSeqAsync`; the listener skips replayed messages at or below it (`ReplicationListenerOptions.UseDeduplication`, on by default).

Routing: Kafka routes by message key with librdkafka's `Murmur2Random` partitioner; with partition-key columns the publisher picks the partition itself with the same hash (`MurmurHash2` in the core project, also used by Event Hubs and Redis). RabbitMQ super streams are routed by the client library.

### Batching

Messages are not confirmed to PostgreSQL individually. Instead, `ReplicationListener` collects up to `BatchSize` messages and calls `ConfirmAsync` (which also calls `Connection.SendStatusUpdate`). If fewer than `BatchSize` messages arrive, the idle confirmation timer fires after `BatchWaitTime` (minimum 10ms, default 100ms).

### `JsonOptions` vs `ReplicationListenerOptions`

- **`JsonOptions`** — Controls what goes in the JSON: nulls, timestamps, table names, write mode (Default/Compact), timestamp format.
- **`ReplicationListenerOptions`** — Controls the listener behavior: slot name, publication names, batch size, partition key columns per table, columns to include per table, initial data copy settings, idle WAL message interval.
