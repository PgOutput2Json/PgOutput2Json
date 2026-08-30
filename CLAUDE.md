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

# Run the manual integration test worker
dotnet run --project src/PgOutput2Json.TestWorker/PgOutput2Json.TestWorker.csproj
```

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
- **`JsonWriter`** — Converts `InsertMessage`, `UpdateMessage` (Default/Full/Index), and `DeleteMessage` into a `JsonMessage`. Supports `Default` and `Compact` write modes. In Compact mode, column names are omitted from row arrays and sent once in a `"s"` schema property when the relation changes.
- **`JsonMessage`** — Reused single instance per `JsonWriter`. Holds `StringBuilder`s for: JSON body, table name, key column value(s), and partition key value(s).
- **`IMessagePublisher` / `MessagePublisher`** — Interface for all output backends. Must implement `PublishAsync`, `ConfirmAsync`, and optionally `GetLastPublishedWalSeqAsync` (used for deduplication — if non-zero, messages with virtual LSN ≤ that value are skipped on reconnect).
- **`IMessagePublisherFactory`** — Creates a new `IMessagePublisher` per connection/slot. All adapter packages implement this.
- **`DataExporter`** — Handles optional initial data copy via PostgreSQL `COPY ... TO STDOUT HEADER`. Tracks progress in a `__pg2j_data_copy_progress` table. Runs before replication starts.

### Publisher Adapter Packages

Each adapter in `src/PgOutput2Json.<Name>/` provides:
- An `IMessagePublisherFactory` implementation
- An extension method on `PgOutput2JsonBuilder` (e.g., `UseRabbitMq(...)`, `UseKafka(...)`)

Available adapters: `RabbitMq`, `RabbitMqStreams`, `Kafka`, `Redis`, `Sqlite`, `MongoDb`, `Kinesis`, `DynamoDb`, `AzureEventHubs`, `Webhooks`.

### Deduplication

Because PostgreSQL can replay WAL on reconnect, messages can arrive twice. Each published message carries a deduplication key: the transaction final LSN ("w" in the JSON) plus a counter ("n") that is reset at every `BeginMessage`. On connect, the listener asks the publisher for the last published position via `GetLastPublishedWalSeqAsync` and skips every message at or below it.

For partitioned publishers (Kafka, RabbitMQ super streams), `GetLastPublishedWalSeqAsync` returns the MINIMUM of the per-partition watermarks — a safe resume point, since everything at or below it is durably published — and the publisher additionally tracks one watermark per partition and skips messages it has already sent to the resolved target partition. Target partitions are computed client-side with a deterministic hash (Kafka murmur2, RabbitMQ murmur3 with seed 104729, the scheme used by `HashRoutingMurmurStrategy`), so routing is stable across restarts. Messages skipped by either level still advance the WAL confirmation.

### Batching

Messages are not confirmed to PostgreSQL individually. Instead, `ReplicationListener` collects up to `BatchSize` messages and calls `ConfirmAsync` (which also calls `Connection.SendStatusUpdate`). If fewer than `BatchSize` messages arrive, the idle confirmation timer fires after `BatchWaitTime` (minimum 10ms, default 100ms).

### `JsonOptions` vs `ReplicationListenerOptions`

- **`JsonOptions`** — Controls what goes in the JSON: nulls, timestamps, table names, write mode (Default/Compact), timestamp format.
- **`ReplicationListenerOptions`** — Controls the listener behavior: slot name, publication names, batch size, partition key columns per table, columns to include per table, initial data copy settings, idle WAL message interval.
