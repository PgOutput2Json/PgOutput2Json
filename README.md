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

## 1. Quick Start

### 1.1 Configure `postgresql.conf`

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

### 1.2 Create a Replication User

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

### 1.3 Create a Publication

Now, connect to the target database and create a publication to specify which tables and actions should be replicated:

```sql
CREATE PUBLICATION my_publication  
    FOR TABLE my_table1, my_table2  
    WITH (publish = 'insert, update, delete');
```

In the example code below, we'll assume the database name is `my_database`.

### 1.4 Create a .NET Worker Service

Set up a new **.NET Worker Service** and add the `PgOutput2Json` package:

```
dotnet add package PgOutput2Json
```

In your `Worker.cs`, add the following:

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
        using var pgOutput2Json = PgOutput2JsonBuilder.Create()  
            .WithLoggerFactory(_loggerFactory)  
            .WithPgConnectionString("server=localhost;database=my_database;username=pgoutput2json;password=_your_password_here_")  
            .WithPgPublications("my_publication")  
            .WithMessageHandler((json, table, key, partition) =>  
            {  
                Console.WriteLine($"{table}: {json}");
                return Task.FromResult(true);
            })  
            .Build();  

        await pgOutput2Json.StartAsync(stoppingToken);  
    }  
}
```

> **Note:** This example uses a **temporary replication slot**, meaning it won’t capture changes made while the worker was stopped.

## 2. Working with Permanent Replication Slots

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

## 3. Using RabbitMQ (Classic)

This document assumes you have a running RabbitMQ instance working on the default port, with the default `guest`/`guest` user allowed from the localhost.

> ⚠️ **Important:** First, set up the database, as described in the **QuickStart** section above.

### 3.1 Create Topic Exchange

In RabbitMQ, create a `durable` `topic` type `exchange` where PgOutput2Json will be pushing JSON messages. We will assume the name of the exchange is `my_exchange` in the `/` virtual host.

### 3.2 Create and Bind a Queue to Hold the JSON Messages

Create a `durable` queue named `my_queue` and bind it to the exchange created in the previous step. Use `public.#` as the routing key. This ensures that it will receive JSON messages for all tables in the `public` schema.

> **Note:** Use a different schema name if your tables are in a different schema.

### 3.3 Create a .NET Worker Service

Set up a new **.NET Worker Service** and add the following package reference:

```
dotnet add package PgOutput2Json.RabbitMq
```

In your `Worker.cs`, add the following code:

```csharp
using PgOutput2Json;
using PgOutput2Json.RabbitMq;

public class Worker : BackgroundService  
{  
    private readonly ILoggerFactory _loggerFactory;  

    public Worker(ILoggerFactory loggerFactory)  
    {  
        _loggerFactory = loggerFactory;  
    }  

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)  
    {  
        // This code assumes PostgreSQL and RabbitMQ are on localhost  
        using var pgOutput2Json = PgOutput2JsonBuilder.Create()  
            .WithLoggerFactory(_loggerFactory)  
            .WithPgConnectionString("server=localhost;database=my_database;username=pgoutput2json;password=_your_password_here_")  
            .WithPgPublications("my_publication")  
            .UseRabbitMq(options =>  
            {  
                options.HostNames = [ "localhost" ];  
                options.Username = "guest";  
                options.Password = "guest";  
                options.VirtualHost = "/";  
                options.ExchangeName = "my_exchange";

                // set to true for persistent messages
                options.UsePersistentMessagesByDefault = false; 
            })  
            .Build();  

        await pgOutput2Json.StartAsync(stoppingToken);  
    }  
}
```

Run the code, and with a little luck, you should see JSON messages being pushed into the `my_queue` in RabbitMQ when you make a change in the tables specified in `my_publication`. 
The routing key will be in the form: `schema.table.key_partition`. Since we did not configure anything specific in the PgOutput2Json, the `key_partition` will always be `0`.

### 3.4 Batch Size

At the end of every batch the listener waits for the confirms of RabbitMQ (publisher confirms). `UseRabbitMq` therefore sets the batch size to 1,000 changes (the default of the library is 100). In our [benchmark](https://github.com/PgOutput2Json/PgOutput2Json/tree/main/src/PgOutput2Json.Benchmark), with a durable queue bound to the exchange, the initial copy and the stream ran at about 31,000 rows per second with batches of 1,000, and at about 24,000 and 21,000 with batches of 100 (measured on a Hetzner CX33, a cost-optimized cloud server with 4 shared vCPUs and 8 GB RAM, with PostgreSQL and RabbitMQ in Docker on the same machine). Larger batches added nothing: RabbitMQ itself used most of the CPU, routing and confirming every message, also when the messages were not persistent. For more throughput, use [RabbitMQ Streams](#4-using-rabbitmq-streams). To use another size, call `WithBatchSize` after `UseRabbitMq`. A batch that is not full is still confirmed after the batch wait time (100 ms by default).

## 4. Using RabbitMQ (Streams)

Using RabbitMQ Streams is similar to the standard RabbitMQ setup, but instead of creating an exchange and binding queues, you need to create a stream, and configure the stream protocol in RabbitMQ.

> ⚠️ **Important:** First, set up the database, as described in the **QuickStart** section above.

### 4.1 Set Up RabbitMQ Streams

Before using RabbitMQ Streams, ensure that the RabbitMQ Streams plugin is enabled. The stream protocol must be active for the stream to work.

Create a stream in RabbitMQ, for example, a stream named `my_stream`. You can do this from the RabbitMQ management interface or through the appropriate RabbitMQ commands.

### 4.2 Create .NET Worker Service

Set up a new **.NET Worker Service** and add the following package reference:

```
dotnet add package PgOutput2Json.RabbitMqStreams
```

In your `Worker.cs`, add the following code to use RabbitMQ Streams:

```csharp
using PgOutput2Json;
using PgOutput2Json.RabbitMqStreams;

public class Worker : BackgroundService  
{  
    private readonly ILoggerFactory _loggerFactory;  

    public Worker(ILoggerFactory loggerFactory)  
    {  
        _loggerFactory = loggerFactory;  
    }  

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)  
    {  
        // This code assumes PostgreSQL and RabbitMQ are on localhost  
        using var pgOutput2Json = PgOutput2JsonBuilder.Create()  
            .WithLoggerFactory(_loggerFactory)  
            .WithPgConnectionString("server=localhost;database=my_database;username=pgoutput2json;password=_your_password_here_")  
            .WithPgPublications("my_publication")  
            .UseRabbitMqStreams(options =>  
            {  
                options.StreamName = "my_stream";  
                options.StreamSystemConfig.UserName = "guest";  
                options.StreamSystemConfig.Password = "guest";  
                options.StreamSystemConfig.VirtualHost = "/";  
                options.StreamSystemConfig.Endpoints =  
                [  
                    new IPEndPoint(IPAddress.Loopback, 5552)  
                ];  
            })  
            .Build();  

        await pgOutput2Json.StartAsync(stoppingToken);  
    }  
}
```

Once the stream is configured in RabbitMQ and the .NET service is running, changes to the PostgreSQL tables specified in `my_publication` will be pushed to the `my_stream` stream in RabbitMQ. 
No need to manage exchanges or queues — everything will flow directly through the stream.

### 4.3 Batch Size

The producer has up to one batch of messages in flight, and the listener waits for their confirmations at the end of the batch. `UseRabbitMqStreams` therefore sets the batch size to 10,000 changes (the default of the library is 100). In our [benchmark](https://github.com/PgOutput2Json/PgOutput2Json/tree/main/src/PgOutput2Json.Benchmark) the initial copy ran at about 170,000 rows per second and the stream at about 100,000 with batches of 10,000, and at about 68,000 and 42,000 with batches of 100 (measured on a Hetzner CX33, a cost-optimized cloud server with 4 shared vCPUs and 8 GB RAM, with PostgreSQL and RabbitMQ in Docker on the same machine). To use another size, call `WithBatchSize` after `UseRabbitMqStreams`. A batch that is not full is still confirmed after the batch wait time (100 ms by default). After an error, the unconfirmed messages of the batch are sent again.

The messages go to the client in lists of `options.MessageBufferSize` (100 by default), each of which is one frame. A super stream gets them one by one, since it routes each message to its partition.

### 4.4 Deduplication

By default the changes that PostgreSQL replays after a reconnect are published again, and consumers deduplicate on `w`/`n`. On a single stream that only this listener writes to, `options.UseDeduplication = true` keeps them out of the stream: when it connects, the publisher reads the position (`w`, `n`) of the last message in the stream, and the listener skips the changes up to it.

It cannot be used with a super stream or with the initial data copy: `Build()` throws. It needs RabbitMQ 3.11 or later.

## 5. Using Kafka

Kafka is supported out of the box and works similarly to the RabbitMQ integrations. PgOutput2Json will publish JSON messages directly to a Kafka topic.

> ⚠️ **Important:** First, set up the database, as described in the **QuickStart** section above.

### 5.1 Set Up Kafka

Ensure you have a running Kafka broker accessible to your .NET application. This example assumes Kafka is running locally on the default port `9092`.

Create a topic in Kafka named `my_topic`. You can do this using Kafka’s command-line tools or through a Kafka UI.

### 5.2 Create .NET Worker Service

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

### 5.3 Batch Size

At the end of every batch the listener waits until Kafka has acknowledged all of its messages (with `acks=all`, which idempotence needs). `UseKafka` therefore sets the batch size to 10,000 changes (the default of the library is 100). In our [benchmark](https://github.com/PgOutput2Json/PgOutput2Json/tree/main/src/PgOutput2Json.Benchmark) the initial copy ran at about 140,000 rows per second and the stream at about 94,000 with batches of 10,000, and at about 8,800 and 8,300 with batches of 100 (measured on a Hetzner CX33, a cost-optimized cloud server with 4 shared vCPUs and 8 GB RAM, with PostgreSQL and Kafka in Docker on the same machine). The batching of librdkafka itself (`linger.ms`, 5 ms by default) made no difference there. To use another size, call `WithBatchSize` after `UseKafka`. A batch that is not full is still confirmed after the batch wait time (100 ms by default). After an error, the unconfirmed messages of the batch are sent again.

## 6. Using Redis

PgOutput2Json supports publishing JSON messages to Redis streams (default) or Pub/Sub channels.

> ⚠️ **Important:** First, set up the database, as described in the **QuickStart** section above.

### 6.1 Set Up Redis

Make sure you have a Redis instance running and accessible to your .NET application. This example assumes Redis is running locally on the default port `6379`.

### 6.2 Create .NET Worker Service

Set up a new **.NET Worker Service** and add the following package reference:

```
dotnet add package PgOutput2Json.Redis
```

In your `Worker.cs`, use the following code to publish change events to Redis:

```csharp
using PgOutput2Json;
using PgOutput2Json.Redis;

public class Worker : BackgroundService  
{  
    private readonly ILoggerFactory _loggerFactory;  

    public Worker(ILoggerFactory loggerFactory)  
    {  
        _loggerFactory = loggerFactory;  
    }  

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)  
    {  
        // This code assumes PostgreSQL and Redis are on localhost  
        using var pgOutput2Json = PgOutput2JsonBuilder.Create()  
            .WithLoggerFactory(_loggerFactory)  
            .WithPgConnectionString("server=localhost;database=my_database;username=pgoutput2json;password=_your_password_here_")  
            .WithPgPublications("my_publication")  
            .UseRedis(options =>  
            {
                options.StreamName = "my_stream";
                options.PublishMode = PublishMode.Stream;
                options.StreamNameSuffix = StreamNameSuffix.None; // or TableName, or TableNameAndPartition
                options.Redis.EndPoints.Add("localhost:6379");  
            })  
            .Build();  

        await pgOutput2Json.StartAsync(stoppingToken);  
    }  
}
```

JSON messages will be published to the specified Redis stream. If a stream name suffix is specified, the stream or channel name becomes dynamic, using the format: `stream_name:schema.table:partition`.

> **Note:** The table name is always qualified with the schema using the `.` character.

## 7. Using SQLite

PgOutput2Json supports copying modified PostgreSQL rows to SQLite. By default, rows are copied only when they change, using logical replication. 
Optionally, initial data copy can be enabled with `WithInitialDataCopy(true)` when configuring the builder.

The PgOutput2Json library will create the SQLite database if it does not already exist, along with any table included in logical replication. A table is created the first time a row belonging to that table is changed. If a table already exists, it is not modified, but new columns will be created automatically, if a new column is added to the source PostgreSQL table.

A table of the `public` schema keeps its name in SQLite (`orders`), a table of another schema is named `schema_table` (`sales_orders`). Set `options.GetTableName` to choose the names yourself: it gets the schema and the table name apart. Two PostgreSQL tables with the same SQLite name are an error. Versions before 2.2 dropped the schema, so tables with the same name in two schemas shared one SQLite table: after the upgrade the rows of a table of another schema than `public` go into a table with the new name, which starts empty. To keep the old names, set `options.GetTableName = (schema, table) => table;`.

The output writes through a connection of its own, opened without pooling, so it neither gets nor leaves connection settings in the pool your application uses (for example `PRAGMA query_only`). On that connection it turns off the foreign key checks, like PostgreSQL's own logical replication: the rows arrive in the order of the table syncs and the batches, not in the order of the foreign keys, so a child row can come before its parent. Your application's connections still check them. To check them on the output's connection too, run `PRAGMA foreign_keys = ON` in `options.PostConnectionSetup`.

Triggers on the tables fire for the replicated rows too: SQLite cannot turn them off for one connection, as PostgreSQL's replication does. A trigger can tell the output's writes apart by the foreign key setting of the connection that fires it:

```sql
CREATE TRIGGER items_audit AFTER UPDATE ON items
WHEN (SELECT foreign_keys FROM pragma_foreign_keys) = 1 -- not the replication
BEGIN
    INSERT INTO audit (item_id) VALUES (new.id);
END;
```

This tells connections with foreign key checks off from those with them on, so it works as long as your application's connections check them (the default of Microsoft.Data.Sqlite and EF Core), and you do not turn them on in `PostConnectionSetup`.

### Dates and times

A `timestamp` (without time zone) is stored as ISO 8601 text without an offset (`2026-01-02T03:04:05.1234560`), so Microsoft.Data.Sqlite and EF Core read it back as the same wall time, with `DateTimeKind.Unspecified`, whatever the time zone of the machine. A `timestamptz` is stored as ISO 8601 text with its offset, in UTC (`2026-01-02T03:04:05.1234560+00:00`). Both columns are created as `DATETIME`. A `date`, `time` or `timetz` is stored as PostgreSQL's text (`2026-01-02`, `03:04:05`, `03:04:05+02`), and a value .NET cannot hold (`infinity`, a date BC) as well.

Up to version 2.1.0, a `timestamp` was stored with the UTC offset of the machine running the output (`+01:00` in Central Europe in January), so it was read back shifted by that offset. Rows already in the database keep that value until they change, or until the table is copied again.

### Batch size

Every batch of changes is one transaction. `UseSqlite` sets the batch size to 1,000 changes (the default of the library is 100). In our [benchmark](https://github.com/PgOutput2Json/PgOutput2Json/tree/main/src/PgOutput2Json.Benchmark) with batches of 1,000 and the database file on the server's disk, the initial copy ran at about 55,000 rows per second and the stream at about 50,000, or about 68,000 and 61,000 with `options.UseWal = true` (the WAL journal mode of SQLite), since a commit then waits less for the disk. With the file in memory (tmpfs), where a commit does not wait for a disk, they ran at about 85,000 and 75,000 (with or without `UseWal`), and at about 72,000 and 68,000 with batches of 100 (measured on a Hetzner CX33, a cost-optimized cloud server with 4 shared vCPUs, 8 GB RAM and its local disk with ext4, with PostgreSQL in Docker on the same machine). Larger batches add little. To use another size, call `WithBatchSize` after `UseSqlite`. A batch that is not full is still written after the batch wait time (100 ms by default).

> ⚠️ **Important:** Be sure to set up the PostgreSQL database first, as described in the **QuickStart** section above.

### 7.1 Create a .NET Worker Service

Set up a new **.NET Worker Service** and add the following package reference:

```
dotnet add package PgOutput2Json.Sqlite
```

In your `Worker.cs`, use the following code to configure change propagation to SQLite:

```csharp
using PgOutput2Json;
using PgOutput2Json.SQLite;

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
            .UseSqlite(options =>
            {
                options.SubscriptionName = "my_subscription"; // one per source database and slot
                options.ConnectionStringBuilder.DataSource = "my_database.s3db";
            }) 
            .Build();  

        await pgOutput2Json.StartAsync(stoppingToken);  
    }  
}
```

## 8. Using MongoDB

PgOutput2Json supports copying modified PostgreSQL rows to MongoDB collections. By default, rows are copied only when they change, using logical replication.
Optionally, initial data copy can be enabled with `WithInitialDataCopy(true)` when configuring the builder.

The PgOutput2Json library will create the MongoDB database if it does not already exist, along with one collection for each table included in logical replication. Collections are created the first time a row belonging to the respective table is changed. Additionally, one unique index is created for each collection, covering the fields that are a part of the primary key. A key field that is an array is stored as its JSON text (`["a","b"]`): a unique index on an array field indexes each element, so two rows whose keys share an element would be duplicates.

A table of the `public` schema gives its name to its collection (`orders`), a table of another schema is named `schema.table` (`sales.orders`). Set `options.GetTableName` to choose the names yourself: it gets the schema and the table name apart, for example `options.GetTableName = (schema, table) => $"{schema}_{table}";`. Two PostgreSQL tables with the same collection name are an error. Versions before 2.2 named the collections `schema__table` (`public__orders`): after the upgrade the rows go into the collections with the new names, which start empty. To keep the old names, set `options.GetTableName = (schema, table) => $"{schema}__{table}";`.

Arrays are stored as BSON arrays. Arrays of enums, user-defined types and domains over arrays are arrays of strings; before version 2.1 they were a string with the PostgreSQL array literal (`{sad,happy}`), and documents written before keep it until their row changes.

A `timestamp` (without time zone) has no time zone in PostgreSQL, but a BSON date is always a moment in UTC. By default the wall time is stored as if it were UTC (`2026-01-02 03:04:05` as `2026-01-02T03:04:05Z`), so the MongoDB driver reads it back as the same wall time, whatever the time zone of the machine running the output: the `Z` only stands for "no time zone". If your timestamps are times of a known zone, set `options.TimestampTimeZone` (for example `TimeZoneInfo.FindSystemTimeZoneById("Europe/Zagreb")`), and they are stored as the moments they are (`2026-01-02T02:04:05Z`), converted as PostgreSQL's `AT TIME ZONE` does: a time skipped when the clock is set forward gets the offset of before, and a time that is there twice the offset of after. A `timestamptz` is stored at its own moment, whatever the option. BSON dates have milliseconds, so the microseconds are cut off. A value a BSON date cannot hold here (`infinity`, a date BC, a year after 9999) is stored as PostgreSQL's text, and a null in an array of timestamps as null. Earlier versions stored a `timestamp` shifted by the UTC offset of the machine running the output (an hour earlier in Central Europe in January), a null in an array of timestamps as 1970-01-01, and stopped the replication on a value they could not read. Documents already written keep their values until their row changes, or until the table is copied again.

A changed row replaces its document (found by the key), so the document has exactly the columns of the row: a field that is not a column of the table is not kept. An update that did not change a large value (TOAST), which PostgreSQL does not send, sets the other fields instead, and keeps that one. Writing a change to MongoDB costs it a look-up of the key and a write of the document, so its own speed is the limit of this output.

> ⚠️ **Important:** Be sure to set up the PostgreSQL database first, as described in the **QuickStart** section above.

### 8.1 Create a .NET Worker Service

Set up a new **.NET Worker Service** and add the following package reference:

```
dotnet add package PgOutput2Json.MongoDb
```

In your `Worker.cs`, use the following code to configure change propagation to MongoDB:

```csharp
using PgOutput2Json;
using MongoDB.Driver;
using MongoDB.Driver.Core.Configuration;

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
            .UseMongoDb(options =>
            {
                options.SubscriptionName = "my_subscription"; // one per source database and slot
                options.DatabaseName = "my_mongo_database";
                options.ClientSettings = new MongoClientSettings
                {
                    Server = new MongoServerAddress("localhost", 27017),
                    Scheme = ConnectionStringScheme.MongoDB,
                    Credential = MongoCredential.CreateCredential("admin", "admin", "__your_mongo_password_here__"),
                };
            })
            .Build();  

        await pgOutput2Json.StartAsync(stoppingToken);  
    }  
}
```

## 9. Using Amazon Kinesis

PgOutput2Json supports pushing row changes as JSON messages to Amazon Kinesis.

> ⚠️ **Important:** Be sure to set up the PostgreSQL database first, as described in the **QuickStart** section above.

### 9.1 Create a .NET Worker Service

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

### 9.2 Records Out of Order After a Rejected Record

A batch of changes is sent in `PutRecords` requests of up to 500 records and 5 MB, one after the other.

Kinesis can accept the records of a request that follow a record it rejected (for example one throttled with `ProvisionedThroughputExceededException`). The batch is not confirmed and is replayed, so the rejected record reaches the stream once the problem is gone, but **after** records that follow it in the WAL. For example, if record 2 of 1, 2, 3 is rejected, the shard ends up with 1, 3, 1, 2, 3.

- A consumer that applies changes idempotently is not affected: a row may show an older version for a moment, and the replay brings it up to date.
- A consumer that keeps a watermark (the highest `(w, n)` pair processed) skips the late record as a duplicate, and loses it. Give the stream enough capacity that records are not throttled (on-demand mode, or enough shards), or apply the changes idempotently.

## 10. Using Amazon DynamoDB

PgOutput2Json supports copying modified PostgreSQL rows to Amazon DynamoDB tables. By default, rows are copied only when they change, using logical replication.
Optionally, initial data copy can be enabled with `WithInitialDataCopy(true)` when configuring the builder.

The PgOutput2Json library will create DynamoDB tables for each table included in logical replication. 

A table of the `public` schema gives its name to its DynamoDB table (`orders`), a table of another schema is named `schema.table` (`sales.orders`). Set `options.GetTableName` to choose the names yourself: it gets the schema and the table name apart. Two PostgreSQL tables with the same DynamoDB name are an error. `TableBillingModes` is by the PostgreSQL table (`schema.table`). Versions before 2.2 named every table `schema.table`, also `public.orders`: after the upgrade the rows of a table of the `public` schema go into a table with the new name, which starts empty. To keep the old names, set `options.GetTableName = (schema, table) => $"{schema}.{table}";`.

Tables are created the first time a row belonging to the respective table is changed. 
The first PK column is used as partition key. The other PK columns are joined with `|` into a string, stored in the sort key attribute `SK`. An array column is stored as a string with its JSON text.

> ⚠️ **Important:** Be sure to set up the PostgreSQL database first, as described in the **QuickStart** section above.

### 10.1. Create a .NET Worker Service

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
### 10.2. Batch Size

Every batch of changes is written with `BatchWriteItem` requests of up to 25 items, and `MaxParallelBatchWrites` of them (8 by default) are in flight at a time. Two changes of the same row are never in flight together, so they are applied in order. The position is saved once per batch, after its rows. `UseDynamoDb` sets the batch size to 1,000 changes (the default of the library is 100), so that the parallel requests have work. In our [benchmark](https://github.com/PgOutput2Json/PgOutput2Json/tree/main/src/PgOutput2Json.Benchmark), against DynamoDB Local, 8 parallel requests were about 2.5 times faster than one (measured on a Hetzner CX33, a cost-optimized cloud server with 4 shared vCPUs and 8 GB RAM, with PostgreSQL and DynamoDB Local in Docker on the same machine). DynamoDB Local is not DynamoDB: on AWS the throughput depends on the network and on the capacity of the tables, so measure it with your own tables (`--dynamodb URL`). To use another size, call `WithBatchSize` after `UseDynamoDb`. A batch that is not full is still written after the batch wait time (100 ms by default).

## 11. Using Azure Event Hubs

### Connection String

You can obtain the Event Hubs connection string from the Azure portal:

1. Navigate to your Event Hubs namespace
2. Go to "Shared access policies"
3. Select or create a policy with "Send" permissions
4. Copy the connection string

### Event Hub Creation

Make sure to create the Event Hub in your namespace before running the application. You can do this through:

- Azure portal
- Azure CLI: `az eventhubs eventhub create --resource-group myResourceGroup --namespace-name myNamespace --name my-eventhub`
- ARM templates
- Terraform

### Partitioning Strategy

The publisher chooses the partition of every event itself (murmur2, as the Kafka output does). By default it hashes the table name, so all the rows of a table go to one partition, in the order of the changes, and the tables are spread over the partitions. With partition-key columns configured for a table (`WithPartitionKeyFields`), it hashes the values of those columns instead, so the rows of the table are spread over the partitions.

At the end of every batch the events are sent once per partition, and the partitions are sent at the same time. Each partition gets its events in WAL order, also the events of different tables.

Up to version 2.0.1, the partition of a table without partition-key columns was chosen by Event Hubs, from the table name as the partition key. After an upgrade, a table can therefore move to another partition. A consumer that keeps a watermark per table and partition must take the watermark of the table from its old partition.

### Create a .NET Worker Service

Set up a new **.NET Worker Service** and add the following package reference:

```
dotnet add package PgOutput2Json.AzureEventHubs
```

In your `Worker.cs`, use the following code to configure change propagation to Azure Event Hubs:

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
            .UseEventHubs(options =>
            {
                // Connection string format: Endpoint=sb://[namespace].servicebus.windows.net/;SharedAccessKeyName=[key-name];SharedAccessKey=[key-value]
                options.ConnectionString = "Endpoint=sb://my-eventhub-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=your-access-key-here";
                options.EventHubName = "my-eventhub";
                
                // Optional: Configure client options
                // options.ClientOptions = new EventHubProducerClientOptions
                // {
                //     ConnectionOptions = new EventHubConnectionOptions
                //     {
                //         TransportType = EventHubsTransportType.AmqpWebSockets
                //     }
                // };
            })
            .Build();  

        await pgOutput2Json.StartAsync(stoppingToken);  
    }  
}
```

> **Note:** This example uses a **temporary replication slot**, meaning it won't capture changes made while the worker was stopped.

## 12. Using Webhooks

### Create a .NET Worker Service

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

### Webhook payload

Changes are delivered in batches to your webhook. The POST request body is an array of these elements:

```jsonc
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

### Webhook metadata

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

### Batch size

Every batch of changes is one POST, and the listener waits for its answer before it goes on. The batch size is the default of the library, 100 changes. In our [benchmark](https://github.com/PgOutput2Json/PgOutput2Json/tree/main/src/PgOutput2Json.Benchmark), with a webhook that only reads the request, the initial copy ran at about 130,000 rows per second and the stream at about 81,000 with batches of 100, at about 184,000 and 86,000 with batches of 1,000, and at about 209,000 and 106,000 with batches of 10,000 (measured on a Hetzner CX33, a cost-optimized cloud server with 4 shared vCPUs and 8 GB RAM, with PostgreSQL in Docker on the same machine and the webhook in the process of the benchmark). A larger batch is a larger request (about 300 bytes a row in the benchmark), so call `WithBatchSize` after `UseWebhook` if your webhook takes it. A batch that is not full is still sent after the batch wait time (100 ms by default).

## 13. Using ClickHouse

PgOutput2Json can keep a copy of your PostgreSQL tables in ClickHouse. By default, rows are copied only when they change, using logical replication.
Optionally, initial data copy can be enabled with `WithInitialDataCopy(true)` when configuring the builder.

The output uses the HTTP interface of ClickHouse, and sends the rows as JSON, or in the faster RowBinary format if you turn it on (see [Insert format](#insert-format)). It needs no ClickHouse client library, and is tested with ClickHouse 26.10.

> ⚠️ **Important:** Be sure to set up the PostgreSQL database first, as described in the **QuickStart** section above.

### How the rows are stored

A table is created the first time one of its rows arrives, as a `ReplacingMergeTree` ordered by the key columns of the PostgreSQL table.

ClickHouse is slow at updating and deleting rows, so the output never does either. Every change is inserted as a new row, with three extra columns:

- `_version`: the position of the change in the PostgreSQL WAL (`w * 2^64 + n` of the [deduplication key](#-delivery-and-deduplication)). A copied row has version 0.
- `_deleted`: 1 if the change is a delete.
- `_synced_at`: when ClickHouse wrote the row, in UTC. It shows how far behind the copy is, and lets a materialized view process only the new rows. A change that PostgreSQL replays is written again, with a new time.

ClickHouse keeps the row with the highest version per key when it merges the parts of a table, which happens in the background at a time you cannot predict. Until then the older versions are still there, so **read the tables with `FINAL`**:

```sql
SELECT * FROM orders FINAL
```

This returns the current rows, without the deleted ones.

A change that PostgreSQL replays after a reconnect has the same key and version as the first time, so it changes nothing. This is why the output does not save its position.

Tables created before version 2.1 have no `_synced_at`, and the output does not add it: ClickHouse would show the time of the query for the rows written before. Add it yourself if that is fine for you:

```sql
ALTER TABLE orders ADD COLUMN _synced_at DateTime64(6, 'UTC') DEFAULT now64(6)
```

### Deleted rows

The row of a delete stays in the table, marked deleted. `FINAL` hides it, but it takes space. With `options.CleanUpDeletedRowsAfter = TimeSpan.FromHours(1)` the output sets the cleanup settings of `ReplacingMergeTree` on its tables (a new table when it is created, an existing one on the next start), and ClickHouse removes the deleted rows in a merge once no new rows have arrived in a table for that long. ClickHouse still marks this feature experimental.

- A table that gets new rows all the time never waits that long. Run `OPTIMIZE TABLE orders FINAL CLEANUP` instead, for example at night: the settings allow it. It rewrites the whole table.
- Once a deleted row is removed, nothing hides an older version of it. When PostgreSQL replays changes after a reconnect, a deleted row can show up again until the replayed delete arrives.
- Setting the option back to null does not remove the settings from the tables.

### Tables

- A table of the `public` schema keeps its name. A table of another schema is named `schema_table`. Set `options.GetTableName` to choose the names yourself: it gets the schema and the table name apart, for example `options.GetTableName = (schema, table) => $"pg_{schema}_{table}";` (before 2.2 it got the joined name). Two PostgreSQL tables with the same ClickHouse name are an error.
- A table that already exists is used as it is. To choose the column types or an engine the options do not offer (for example a replicated one), create the table before the first row arrives. It needs the columns `_version UInt128` and `_deleted UInt8`. With RowBinary, a column type it does not cover makes the rows of the table go as JSON: see [Insert format](#insert-format).
- A column added in PostgreSQL is added in ClickHouse. A column dropped in PostgreSQL stays. A changed column type is not followed: change the ClickHouse table yourself. Neither is a changed primary key definition (other key columns), because ClickHouse cannot change the order of an existing table: drop the ClickHouse table and copy it again. An update that changes the key value of a row is followed.
- The database must exist.

### Order, partitions and engine

`options.Tables` sets them per PostgreSQL table:

```csharp
.UseClickHouse(options =>
{
    options.Tables["public.orders"] = new ClickHouseTableOptions
    {
        OrderBy = ["tenant_id", "id"],
        PartitionBy = "intDiv(tenant_id, 1000)",
    };
    options.Tables["public.events"] = new ClickHouseTableOptions { Engine = ClickHouseTableEngine.MergeTree };
})
```

- `OrderBy`: the key columns, in the order you want. The default is the order of the table.
- `PartitionBy`: a `PARTITION BY` expression. The default is no partitions. The cleanup of [deleted rows](#deleted-rows) works per partition: a partition that has had no new rows (inserts, updates or deletes) for that long is cleaned up, even while other partitions of the table get new rows.
- `Engine`:
  - `ReplacingMergeTree` (the default) keeps the latest version of every row.
  - `MergeTree` keeps every version, deletes included: the history of the table, in the order of `_version`. Read it without `FINAL`. A change that PostgreSQL replays after a reconnect is there twice, with the same `_version`.
  - `Null` keeps nothing: only the materialized views on the table get the rows. An update that did not change a large value has `null` for it, because there is no row to take it from, unless the table has `REPLICA IDENTITY FULL`.

ClickHouse replaces and deletes the rows by the `ORDER BY`, and only within a partition. The row of a delete has only the key columns. So `OrderBy` must have exactly the key columns, and `PartitionBy` may use only key columns; otherwise the output stops with an error when it creates the table. The key columns are those of the replica identity. To sort or partition by another column, make it part of the replica identity, for example:

```sql
CREATE UNIQUE INDEX orders_tenant_id ON orders (tenant_id, id);
ALTER TABLE orders REPLICA IDENTITY USING INDEX orders_tenant_id;
```

A table without a key gets only inserts, so it can be ordered and partitioned by any of its columns.

The options are used only when the output creates the table: ClickHouse cannot change the order, the partitions or the engine of an existing table. To change them, drop the ClickHouse table and copy it again.

### Column types

| PostgreSQL | ClickHouse |
|---|---|
| `boolean` | `Bool` |
| `smallint`, `integer`, `bigint` | `Int16`, `Int32`, `Int64` |
| `real`, `double precision` | `Float32`, `Float64` (`NaN` and `Infinity` are null) |
| `numeric(p, s)` | `Decimal(p, s)` |
| `numeric` without precision | `Decimal(76, 38)` |
| `uuid` | `UUID` |
| `date` | `Date32` |
| `timestamp` | `DateTime64(6, 'UTC')`, or the zone of `TimestampTimeZone` |
| `timestamptz` | `DateTime64(6, 'UTC')` |
| arrays of the types above, and of `text`, `bytea` and the other types sent as text | `Array(Nullable(...))` of the element type, e.g. `integer[]` is `Array(Nullable(Int32))` (see [Arrays](#arrays)) |
| arrays of enums and user-defined types | `Array(Nullable(String))`, e.g. `['sad','happy']` |
| `json`, `jsonb` | `JSON` (see [JSON columns](#json-columns)) |
| `bytea` | `String` with the bytes in hex |
| anything else (`text`, `time`, `interval`, enums...) | `String` with the text PostgreSQL sends |

Key columns are not `Nullable`, all the other columns are, except arrays. A key column that is an array, or json, is a `String`.

### Arrays

An array is a ClickHouse array, so `has()`, `arrayJoin()` and the other array functions work on it without parsing. Two things differ from PostgreSQL:

- **A null array is an empty array**, because ClickHouse arrays cannot be `Nullable`. A `NULL` element stays `null`.
- **One dimension.** PostgreSQL does not send the number of dimensions of a column (`integer[]` and `integer[][]` are the same type), so the ClickHouse column has one. A multi-dimensional value stops the replication with an error that names the column. Change that column to a `String` (`ALTER TABLE ... MODIFY COLUMN ... String`), and the listener continues with the JSON text (for an array of an enum or a user-defined type, the PostgreSQL array literal).

The library sends the arrays of enums and user-defined types as the PostgreSQL array literal (`{sad,happy}`). The output knows they are arrays from the name of the type that PostgreSQL sends with the schema (an array type is named after its element type with an underscore: `_mood`), and stores their elements as strings. A domain over an array, such as `CREATE DOMAIN ints AS int[]`, is also an `Array(Nullable(String))`.

With `options.UseArrayTypes = false`, new array columns are `String`s with the JSON text, as before version 2.1, and a null array stays `null`. An existing column keeps its type.

### JSON columns

`json` and `jsonb` columns are stored in the `JSON` type of ClickHouse, which needs ClickHouse 25.3 or newer. With `options.UseJsonType = false` they are `String` columns with the JSON text.

The `JSON` type holds only objects, and rejects some values PostgreSQL accepts. The output changes those values, so that no row is rejected:

- A value that is not an object (an array, a number, a string, `true`, a JSON `null`) is stored as `{"v": value}`. ClickHouse does not store a null inside an object, so a JSON `null` becomes `{}`.
- A key that is twice in an object of a `json` column keeps its last value, as in `jsonb`.
- A number too large for a double (above about 1.8e308) is stored as a string.
- Two keys with the same path, such as `"a.b"` and `"b"` inside `"a"`, keep one value.

ClickHouse stores a number with a fraction as a double, and an integer of more than 64 bits as a string.

A `json` column that is part of the key is a `String`, as is an array of `json`.

### Limits

- **Unchanged large values (TOAST).** PostgreSQL does not send a large value that an update did not change, and the new row in ClickHouse needs it. The output reads it from ClickHouse, with one query per such update. Many updates of rows with large values are slow.
- **`REPLICA IDENTITY FULL`.** Every column is part of the key then, so two rows with the same values in every column are one row in ClickHouse.
- **Tables without a key.** PostgreSQL publishes only their inserts. They are created as a plain `MergeTree`: read them without `FINAL`, and expect a row twice if its insert is replayed after a reconnect.
- **Numbers.** A `numeric` without precision has room for 38 digits before the decimal point. A larger number is rejected by ClickHouse.
- **Dates.** `infinity`, dates before Christ and years above 9999 do not exist in ClickHouse. They are stored as the first or the last date ClickHouse has (year 1 or 9999; ClickHouse 25 and older: 1900 or 2299).
- **Rejected rows.** A row that ClickHouse rejects stops the replication, as with every output: nothing is skipped. Fix the ClickHouse table (for example, change the column to `String`) and the listener continues at its next attempt.

### Batch size

ClickHouse wants few, large inserts. `UseClickHouse` therefore sets the batch size to 10,000 changes (the default of the library is 100). In our benchmark the initial copy ran at about 1,500 rows per second with batches of 100 and at about 95,000 with batches of 10,000, or about 105,000 with RowBinary (see [Insert format](#insert-format)) (measured on a Hetzner CX33, a cost-optimized cloud server with 4 shared vCPUs and 8 GB RAM, with PostgreSQL and ClickHouse in Docker on the same machine). To use another size, call `WithBatchSize` after `UseClickHouse`. A batch that is not full is still sent after the batch wait time (100 ms by default).

With `options.UseAsyncInserts` (on by default) a batch of fewer than 1,000 rows is an asynchronous insert: ClickHouse collects the rows of several inserts before it writes them, which keeps the number of parts low when changes arrive a few at a time. The insert is still acknowledged only when the rows are written, which adds some 50 to 200 ms to the batch. A larger batch is written at once, since the output sends one insert at a time and would only wait for ClickHouse. Turn the option off for the lowest latency of small batches.

## Insert format

By default the rows are sent as JSON (`JSONCompactEachRow`), and ClickHouse parses every value itself: what it stores is what its own parser makes of the value.

With `options.UseRowBinary = true` they are sent in RowBinary, which ClickHouse reads two to three times faster (measured on a Hetzner CX33, a cost-optimized cloud server with 4 shared vCPUs and 8 GB RAM, with PostgreSQL and ClickHouse in Docker on the same machine). In our [benchmark](https://github.com/PgOutput2Json/PgOutput2Json/tree/main/src/PgOutput2Json.Benchmark) it made the initial copy and the stream about 10% faster on the same machine, since the inserts are only a part of the time. It is newer, and off by default, as the binary format of PostgreSQL's own replication: the output then does what the parser of ClickHouse does with the text (the time zone of a `timestamp`, dates out of range, decimal digits), so a difference would be stored without an error.

RowBinary has no conversion: every value must be of the type the column has. So the output reads the column types of each table from ClickHouse when it starts, and writes each value as that type. It covers the types it creates, also when an existing table has them differently (another integer width, `Nullable` or not, another `DateTime64` precision or time zone), and also every integer (`Int8` to `Int256`, `UInt8` to `UInt256`), `Date`, `DateTime`, `Enum8`, `Enum16`, `FixedString`, `IPv4`, `IPv6` and `LowCardinality` of these. Values ClickHouse cannot store are handled as with the text: a date out of the range of the type is its first or its last one, and a value that is not in an `Enum`, too long for a `FixedString`, or an address with a netmask (an `inet` like `10.0.0.0/8`, or a `cidr`) stops the replication with an error that names the column.

A table with any other column type (for example `Map`, `Tuple` or `Nested`) gets its rows as JSON (`JSONCompactEachRow`), which ClickHouse converts to the type of the column. The output logs this when it prepares the table. So does a table with a `DateTime` or `DateTime64` without a time zone when the time zone of the server is not known to .NET.

A `timestamp` (without time zone) is stored as a time of `options.TimestampTimeZone` (UTC by default, for example `TimeZoneInfo.FindSystemTimeZoneById("Europe/Zagreb")`): the output creates its column with that time zone, so the time zone of the ClickHouse server is not used. The output converts the values itself, also in JSON, as PostgreSQL's `AT TIME ZONE` does: a time skipped when the clock is set forward gets the offset of before, and a time that is there twice the offset of after (the parser of ClickHouse takes the summer offset in both cases). Existing columns are not changed: a `DateTime64` without a time zone, which earlier versions created, keeps the time zone of the server, and its values are still sent as text.

### Create a .NET Worker Service

Set up a new **.NET Worker Service** and add the following package reference:

```
dotnet add package PgOutput2Json.ClickHouse
```

In your `Worker.cs`, use the following code to configure change propagation to ClickHouse:

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
        // This code assumes PostgreSQL and ClickHouse are running on localhost
        using var pgOutput2Json = PgOutput2JsonBuilder.Create()
            .WithLoggerFactory(_loggerFactory)
            .WithPgConnectionString("server=localhost;database=my_database;username=pgoutput2json;password=_your_password_here_")
            .WithPgPublications("my_publication")
            .WithInitialDataCopy(true)
            .UseClickHouse(options =>
            {
                options.Endpoint = "http://localhost:8123";
                options.Database = "default";
                options.Username = "default";
                options.Password = "_your_password_here_";
            })
            .Build();

        await pgOutput2Json.StartAsync(stoppingToken);
    }
}
```

### Options

| Option | Default | |
|---|---|---|
| `Endpoint` | `http://localhost:8123` | The HTTP interface of ClickHouse. |
| `Database` | `default` | The database of the tables. It must exist. |
| `Username`, `Password` | `default`, empty | |
| `UseAsyncInserts` | `true` | For batches of fewer than 1,000 rows. See [Batch size](#batch-size). |
| `UseRowBinary` | `false` | See [Insert format](#insert-format). |
| `UseJsonType` | `true` | See [JSON columns](#json-columns). |
| `UseArrayTypes` | `true` | See [Arrays](#arrays). |
| `CleanUpDeletedRowsAfter` | none | Removes the deleted rows. See [Deleted rows](#deleted-rows). |
| `Tables` | empty | The order, partitions and engine per table. See [Order, partitions and engine](#order-partitions-and-engine). |
| `Settings` | empty | ClickHouse settings sent with every request, e.g. `options.Settings["insert_quorum"] = "2"`. |
| `GetTableName` | see [Tables](#tables) | Returns the ClickHouse table name for a PostgreSQL table, from its schema and its name. |
| `HttpClient` | none | Your own `HttpClient`, e.g. with a proxy or a client certificate. |
