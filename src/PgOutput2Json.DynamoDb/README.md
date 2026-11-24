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

The outputs that store data instead of forwarding messages (SQLite, MongoDB, DynamoDB) save their position with the data, and the changes replayed after a reconnect are skipped for them. They keep it per subscription, under `options.SubscriptionName`, which they require: give every source database and slot that writes into the same store its own name. A store of an earlier version has one position, which the first subscription takes. If a source database is restored from a backup or replaced, its positions start again lower, and the saved position would skip its changes: stop the listener, delete the position (the key `Position:<subscription name>` in `__pg2j_config`: a row in SQLite, a document in MongoDB, an item in DynamoDB), and start it again. Its changes are then written again from where the slot is. The message outputs publish them again, except a single RabbitMQ stream with [`UseDeduplication`](https://pg2json.com/docs/outputs/rabbitmq-streams/#deduplication). The ClickHouse output saves no position: every row it writes carries the position as its version, so a replayed change changes nothing.

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

## Using Amazon DynamoDB

PgOutput2Json supports copying modified PostgreSQL rows to Amazon DynamoDB tables. By default, rows are copied only when they change, using logical replication.
Optionally, initial data copy can be enabled with `WithInitialDataCopy(true)` when configuring the builder.

The PgOutput2Json library will create DynamoDB tables for each table included in logical replication.

A table of the `public` schema gives its name to its DynamoDB table (`orders`), a table of another schema is named `schema.table` (`sales.orders`). Set `options.GetTableName` to choose the names yourself: it gets the schema and the table name apart. Two PostgreSQL tables with the same DynamoDB name are an error. `TableBillingModes` is by the PostgreSQL table (`schema.table`). Versions before 2.2 named every table `schema.table`, also `public.orders`: after the upgrade the rows of a table of the `public` schema go into a table with the new name, which starts empty. To keep the old names, set `options.GetTableName = (schema, table) => $"{schema}.{table}";`.

Tables are created the first time a row belonging to the respective table is changed.
The first PK column is used as partition key. The other PK columns are joined with `|` into a string, stored in the sort key attribute `SK`. An array column is stored as a string with its JSON text.

### Batch size

Every batch of changes is written with `BatchWriteItem` requests of up to 25 items, and `MaxParallelBatchWrites` of them (8 by default) are in flight at a time. Two changes of the same row are never in flight together, so they are applied in order. The position is saved once per batch, after its rows. `UseDynamoDb` sets the batch size to 1,000 changes (the default of the library is 100), so that the parallel requests have work. In our [benchmark](https://github.com/PgOutput2Json/PgOutput2Json/tree/main/src/PgOutput2Json.Benchmark), against DynamoDB Local, 8 parallel requests were about 2.5 times faster than one (measured on a Hetzner CX33, a cost-optimized cloud server with 4 shared vCPUs and 8 GB RAM, with PostgreSQL and DynamoDB Local in Docker on the same machine). DynamoDB Local is not DynamoDB: on AWS the throughput depends on the network and on the capacity of the tables, so measure it with your own tables (`--dynamodb URL`). To use another size, call `WithBatchSize` after `UseDynamoDb`. A batch that is not full is still written after the batch wait time (100 ms by default).

### Create a .NET Worker Service

Set up a new **.NET Worker Service** and add the following package reference:

```
dotnet add package PgOutput2Json.DynamoDb
```

In your `Worker.cs`, use the following code to configure change propagation to Amazon DynamoDB:

```csharp
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;
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
            .UseDynamoDb(options =>
            {
                options.SubscriptionName = "my_subscription"; // one per source database and slot
                options.ClientConfig = new AmazonDynamoDBConfig
                {
                    ServiceURL = "http://localhost:8000",                              // for local DynamoDB, change/remove for AWS
                    UseHttp = true,                                                    // for local DynamoDb, remove for AWS
                    DefaultAWSCredentials = new BasicAWSCredentials("dummy", "dummy"), // for local DynamoDB, remove for AWS
                    MaxErrorRetry = 3,                                                 // retry failed requests up to 3 times
                };
            })
            .Build();

        await pgOutput2Json.StartAsync(stoppingToken);
    }
}
```

> **Note:** This example uses a **temporary replication slot**, meaning it won’t capture changes made while the worker was stopped.

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
