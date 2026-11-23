# PgOutput2Json

**PgOutput2Json** is a .NET library that uses **PostgreSQL logical replication** to stream **JSON-encoded change events** from your database to a .NET application or a message broker like **RabbitMQ**, **Kafka**, or **Redis**.

📖 **Documentation:** [pg2json.com](https://pg2json.com/)

## 🚀 Use Cases

Use `PgOutput2Json` when you need to react to DB changes in near real-time:

- 🔄 Background tasks triggered by inserts/updates
- ⚡ Low-latency application cache updates
- 📝 Asynchronous logging (to file or another DB)
- 📊 Real-time data aggregation
- 🛠️ Lightweight ETL for DWH solutions

All with **minimal latency** — events are dispatched shortly after a transaction is committed, though large transactions may introduce additional delay due to processing and transmission overhead.

## 🔌 Supported Outputs

- ✅ **.NET application** via a simple delegate handler
- ✅ **Kafka** (used by [PgHook.Kafka](https://github.com/PgHookCom/PgHook.Kafka))
- ✅ **RabbitMQ** (Streams + Classic Queues - used by [PgHook.RabbitMqStreams](https://github.com/PgHookCom/PgHook.RabbitMqStreams))
- ✅ **Redis** (Streams + Pub/Sub Channels - used by [PgHook.Redis](https://github.com/PgHookCom/PgHook.Redis))
- ✅ **SQLite** (used by [PgFreshCache](https://github.com/PgOutput2Json/PgFreshCache))
- ✅ **MongoDB**
- ✅ **Amazon Kinesis**
- ✅ **Amazon DynamoDB**
- ✅ **ClickHouse**
- ✅ **Azure Event Hubs**
- ✅ **Webhooks** (used by [PgHook](https://github.com/PgHookCom/PgHook))

Plug-and-play adapters handle the heavy lifting — or handle messages directly in your app for maximum control.

## 🧠 How It Works

PostgreSQL 10+ ships with a built-in logical decoding plugin called `pgoutput`, which emits row-level changes (INSERT/UPDATE/DELETE) over a replication slot.

`PgOutput2Json`:

1. Connects to a replication slot via `pgoutput`
2. Converts change events into clean JSON
3. Sends them to your application or a supported message broker

No extra plugins needed — `pgoutput` comes with PostgreSQL by default.

PgOutput2Json needs **PostgreSQL 14 or newer**, and is tested with versions 14 to 18. Row filters and column lists in publications need PostgreSQL 15.

The change events JSON format:
```
{
  "c": "U",             // Change type: I (insert), U (update), D (delete), C (row of the initial data copy)
  "w": 2485645760,      // Transaction final LSN — part of the deduplication key
  "n": 1,               // Message number within the transaction — part of the deduplication key
  "t": "schema.table",  // Table name (if enabled in JSON options)
  "k": { ... },         // Key values — included for deletes, and for updates if the key changed,
                        // or old row values, if the table uses REPLICA IDENTITY FULL
  "r": { ... }          // New row values (not present for deletes)
}
```

The table name is `schema.table`. A schema or a table name that has a dot in it is quoted, with its double quotes doubled: `"sales.eu".orders`, `public."order.items"`, so two tables are never one name. The same name is used in `WithPgColumns` and `WithPartitionKeyFields`. Versions before 2.2 joined these names without quotes: a table with a dot in its schema or table name gets a new name, and with the initial data copy it is copied again.

### Message Key

Besides the JSON, every message has a key: the values of the table's key columns, e.g. `[1]`. Depending on the output, it is the message key (Kafka), the routing or partition key (RabbitMQ, Redis, Kinesis, and Kafka without partition-key columns), or the message id (Azure Event Hubs, RabbitMQ Streams).

The key columns are the **replica identity** columns PostgreSQL sends with the change. The replication protocol does not carry the primary key, and the library does not query the database while streaming, so:

- `REPLICA IDENTITY DEFAULT`: the primary key, stable for all changes to a row.
- `REPLICA IDENTITY USING INDEX`: the index columns.
- `REPLICA IDENTITY FULL`: **all columns**, so the key changes with every update. Kafka log compaction, and routing by key, then treat each version of a row as a different row. Partition-key columns (`WithPartitionKeyFields`) keep the routing stable, but the Kafka message key still changes.
- `REPLICA IDENTITY NOTHING`: empty.

If the message key matters, use `DEFAULT`, or `USING INDEX` on a stable unique key. The rows of the initial data copy have the same key columns as the streamed changes.

### Values

| PostgreSQL type | JSON |
|---|---|
| `smallint`, `integer`, `bigint`, `oid`, `numeric`, `real`, `double precision` | A number, with every digit PostgreSQL sent. `NaN`, `Infinity` and `-Infinity` are `null`, because JSON has no such numbers. |
| `boolean` | `true` or `false` |
| `bytea` | A string of hex digits, e.g. `"deadbeef"` |
| Everything else, including `json`, `jsonb`, `uuid`, dates and timestamps | A string, in PostgreSQL's text format. `timestamptz` is in UTC. |
| Arrays of the types above | A JSON array, nested for more dimensions. A `NULL` element is `null`. |
| Arrays of other types (enums, user-defined types) | A string with the PostgreSQL array literal, e.g. `"{sad,happy}"` |

A large value that an update did not change is not sent by PostgreSQL. It is written as `"__TOAST__"`.

### Compact Mode

Compact mode is off by default. Your message handler, the message brokers and the webhooks get `"k"` and `"r"` as objects with the column names, as shown above, unless you turn compact mode on.

With `.WithJsonOptions(o => o.WriteMode = JsonWriteMode.Compact)`, `"k"` and `"r"` are arrays of values without column names, which makes the messages smaller. The column names come in a schema property:

```
"s": ["schema.table", ["id", 1, 23], ["name", 0, 1043, 104], ["moods", 0, 16384, -1, "_mood"], ...]
                       // column name, part of the key (1/0), type OID, type modifier (only if present),
                       // type name (only for types that are not built in)
```

The schema is not repeated in every message. It comes with the first row of a table after the listener connects, and again after the table changes. After a reconnect it is not always repeated for every table. **Consumers must remember the last schema per table** and decode rows with it, as the SQLite, MongoDB and DynamoDB adapters do. A consumer that starts reading in the middle of a stream must already know the schema, for example from its own store.

Partitioned outputs (Kafka topics with more than one partition, RabbitMQ super streams, and Azure Event Hubs with partition-key columns) spread the rows of a table across partitions, so the schema lands only in the partition of the row it comes with. Consumers must therefore share the last schema per table across all partitions, for example in a common store.

The outputs that store the rows (SQLite, MongoDB, DynamoDB, ClickHouse) work with compact rows internally, whatever `WriteMode` is set to. You never see that JSON, and there is nothing to configure for them.

## 🔁 Delivery and Deduplication

Delivery is **at-least-once**. A batch of changes is confirmed to PostgreSQL only after the output has acknowledged it. After a failure or a restart, PostgreSQL replays everything that was not confirmed, and those changes are published again, in their original order.

A replay is usually one batch, but it can be more:

- A transaction that was interrupted part-way is replayed from its start.
- The position of a replication slot is persisted only at checkpoints, so after a PostgreSQL crash the slot can go back further.

Every message carries a deduplication key: the transaction final LSN (`"w"`) plus the message number within that transaction (`"n"`). Consumers use it in one of two ways:

- **Apply changes idempotently.** Upsert and delete by key. Replayed changes arrive in order, so the result is the same.
- **Keep a watermark.** Remember the highest `(w, n)` pair processed and skip anything at or below it. Keep one watermark per table, in each partition or stream you read: the order is guaranteed only there. On Kafka and Kinesis, a message the output rejects can arrive late and out of order; see those outputs.

The rows of the initial data copy have change type `"C"` and no position: `"w"` and `"n"` are 0. Upsert them by key and leave them out of the watermark. A copy that failed and started again publishes its rows again. While a table is being copied and synced, its changes are published behind the changes of the other tables, which is one more reason to keep the watermark per table.

The outputs that store data instead of forwarding messages (SQLite, MongoDB, DynamoDB) save their position with the data, and the changes replayed after a reconnect are skipped for them. The message outputs publish them again, except a single RabbitMQ stream with [`UseDeduplication`](https://pg2json.com/docs/outputs/rabbitmq-streams/#deduplication). The ClickHouse output saves no position: every row it writes carries the position as its version, so a replayed change changes nothing.

## 🧪 Testing

Every output is covered by integration tests, which run on each change against PostgreSQL 14 to 18.

To measure the throughput on your own machine, run the [benchmark](https://github.com/PgOutput2Json/PgOutput2Json/tree/main/src/PgOutput2Json.Benchmark): it copies and streams rows to a null output or to ClickHouse, and reports rows per second, CPU and allocations.

## 🤖 AI-assisted Development

For transparency: as of recently, we use AI tools (e.g. Claude Code) for some development tasks — writing and reviewing code, tests, and documentation, and analyzing issues. AI-assisted changes go through the same review and testing as everything else before they are merged into a release.

## Quick Start

### Configure `postgresql.conf`

To enable logical replication, add the following setting in your `postgresql.conf`:

```
wal_level = logical

# If needed increase the number of WAL senders, replication slots.
# The default is 10 for both.
max_wal_senders = 10
max_replication_slots = 10
```

Other necessary settings usually have appropriate default values for a basic setup.

> **Note:** PostgreSQL must be **restarted** after modifying this setting.

### Create a Replication User

Next, create a user in PostgreSQL that will be used for replication. Run the following SQL command:

```sql
CREATE USER pgoutput2json WITH  
    PASSWORD '_your_password_here_'  
    REPLICATION;
```

If you are connecting from a remote machine, don't forget to modify `pg_hba.conf` to allow non-local connections for this user. After modifying `pg_hba.conf`, apply the changes by either:

- Sending a **SIGHUP** to the PostgreSQL process  
- Running `pg_ctl reload`  
- Or executing: `SELECT pg_reload_conf();`

### Create a Publication

Now, connect to the target database and create a publication to specify which tables and actions should be replicated:

```sql
CREATE PUBLICATION my_publication  
    FOR TABLE my_table1, my_table2  
    WITH (publish = 'insert, update, delete');
```

In the example code below, we'll assume the database name is `my_database`.

## Using Kafka

Kafka is supported out of the box and works similarly to the RabbitMQ integrations. PgOutput2Json will publish JSON messages directly to a Kafka topic.

### Set Up Kafka

Ensure you have a running Kafka broker accessible to your .NET application. This example assumes Kafka is running locally on the default port `9092`.

Create a topic in Kafka named `my_topic`. You can do this using Kafka’s command-line tools or through a Kafka UI.

### Create .NET Worker Service

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

> **Note:** This example uses a **temporary replication slot**, meaning it won’t capture changes made while the worker was stopped.

### Delivery Guarantees

Messages are confirmed to PostgreSQL in batches, and only after Kafka has acknowledged every message in the batch. If the batch fails, it is not confirmed: the publisher reconnects and PostgreSQL replays the unconfirmed changes.

Delivery is at-least-once: after a failure, the replayed changes are published to Kafka again, and consumers deduplicate on the `w`/`n` fields. Unless you set them yourself, the publisher configures the producer with:

| Setting | Value | Why |
|---|---|---|
| `EnableIdempotence` | `true` | The broker rejects out-of-sequence writes, so retries cannot reorder or duplicate messages. |
| `MessageTimeoutMs` | `0` (infinite) | librdkafka times out each message separately. A timed-out message is skipped while later ones are still delivered, which leaves a gap. |
| `Partitioner` | `Murmur2Random` | The partitioner of the Java client, so a key lands in the same partition as with other Kafka producers. |

The time limit applies to the whole batch instead, through `KafkaPublisherOptions.ConfirmTimeout` (5 minutes by default). While Kafka is unavailable, the confirm waits and logs a warning every 30 seconds. When the timeout expires, the producer is discarded and the listener reconnects.

Things to be aware of:

- Idempotence requires `acks=all`, at most 5 in-flight requests and retries above 0. If you set an incompatible value explicitly (for example `Acks = Acks.Leader`), the producer fails to start.
- If a message is permanently rejected (for example, it is larger than the topic's `max.message.bytes`), the publisher keeps failing on that message until the problem is fixed. Skipping it would lose the change. Each attempt publishes the other messages of the unconfirmed batch again.
- While the confirm waits, no further changes are read, and PostgreSQL retains the WAL for the replication slot until the batch is confirmed. A long Kafka outage therefore grows `pg_wal` on the database server.

#### Messages out of order after a rejected message

When the broker rejects a message, Kafka can still accept the messages sent after it. The batch is not confirmed and is replayed, so the rejected message reaches the topic once the problem is fixed, but **after** messages that follow it in the WAL. For example, if message 2 of 1, 2, 3 is rejected, the partition ends up with 1, 3, 1, 2, 3.

- A consumer that applies changes idempotently is not affected: a row may show an older version for a moment, and the replay brings it up to date.
- A consumer that keeps a watermark (the highest `(w, n)` pair processed) skips the late message as a duplicate, and loses it.

To prevent this, set `ProducerConfig.EnableGaplessGuarantee = true`. librdkafka then stops the producer with a fatal error instead of sending anything after a failed message, and the publisher reconnects and replays in order. It is not enabled by default because librdkafka marks the setting as experimental ("subject to change or removal") and logs a warning on start when it is set.

#### Routing

Messages are routed by the message key. With partition key columns configured for a table (`WithPartitionKeyFields`), its rows are routed by those columns instead, with the same hash (murmur2). Messages without a key go to a random partition.

### Batch size

At the end of every batch the listener waits until Kafka has acknowledged all of its messages (with `acks=all`, which idempotence needs). `UseKafka` therefore sets the batch size to 10,000 changes (the default of the library is 100). In our [benchmark](https://github.com/PgOutput2Json/PgOutput2Json/tree/main/src/PgOutput2Json.Benchmark) the initial copy ran at about 140,000 rows per second and the stream at about 94,000 with batches of 10,000, and at about 8,800 and 8,300 with batches of 100 (measured on a Hetzner CX33, a cost-optimized cloud server with 4 shared vCPUs and 8 GB RAM, with PostgreSQL and Kafka in Docker on the same machine). The batching of librdkafka itself (`linger.ms`, 5 ms by default) made no difference there. To use another size, call `WithBatchSize` after `UseKafka`. A batch that is not full is still confirmed after the batch wait time (100 ms by default). After an error, the unconfirmed messages of the batch are sent again.

## Working with Permanent Replication Slots

If you want to capture changes made while the worker is stopped, you need a permanent replication slot. 

In the context of logical replication, a slot represents a stream of changes that can be replayed to a client in the order they were made on the origin server. 
Each slot streams a sequence of changes from a single database.

To create a replication slot, call the `pg_create_logical_replication_slot` function in the same database that holds the tables being tracked:

```sql
SELECT * FROM pg_create_logical_replication_slot('my_slot', 'pgoutput');
```

The first parameter is the name of the slot, **that must be unique across all databases in a PostgreSQL cluster**. Make sure you specify `pgoutput` as the second parameter - that specifies the correct logical decoding plugin.

The current position of each slot is persisted only at checkpoint, so in the case of a crash, the slot may return to an earlier log sequence number (LSN), which will then cause recent changes to be sent again when the server restarts. 

> **Note:** It is the responsibility of the receiving client to avoid the ill effects from handling the same message more than once.

⚠️ **Important:** Replication slots persist across crashes and know nothing about the state of their consumer(s). They will prevent removal of required resources even when there is no connection using them. This consumes storage because neither required WAL nor required rows from the system catalogs can be removed by VACUUM as long as they are required by a replication slot. 

**In extreme cases, this could cause the database to shut down to prevent transaction ID wraparound.**

**So, if a slot is no longer required, it should be dropped.** To drop a replication slot, use:

```sql
SELECT * FROM pg_drop_replication_slot('my_slot');
```

Once the replication slot is created, to use it, simply specify the name of the slot in the `PgOutput2JsonBuilder`:

```csharp
// ...
using var pgOutput2Json = PgOutput2JsonBuilder.Create()
    .WithLoggerFactory(_loggerFactory)
    .WithPgConnectionString("server=localhost;database=my_database;username=pgoutput2json;password=_your_password_here_")
    .WithPgPublications("my_publication")
    .WithPgReplicationSlot("my_slot")      // <-- slot specified here
//...
```
