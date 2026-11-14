# PgOutput2Json

**PgOutput2Json** is a .NET library that uses **PostgreSQL logical replication** to stream **JSON-encoded change events** from your database to a .NET application or a message broker like **RabbitMQ**, **Kafka**, or **Redis**.

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

### Message Key

Besides the JSON, every message has a key: the values of the table's key columns, e.g. `[1]`. Depending on the output, it is the message key (Kafka), the routing or partition key (RabbitMQ, Redis, Kinesis, and Kafka without partition-key columns), or the message id (Azure Event Hubs, RabbitMQ Streams).

The key columns are the **replica identity** columns PostgreSQL sends with the change. The replication protocol does not carry the primary key, and the library does not query the database while streaming, so:

- `REPLICA IDENTITY DEFAULT`: the primary key, stable for all changes to a row.
- `REPLICA IDENTITY USING INDEX`: the index columns.
- `REPLICA IDENTITY FULL`: **all columns**, so the key changes with every update. Kafka log compaction, and routing by key, then treat each version of a row as a different row. Partition-key columns (`WithPartitionKeyFields`) keep the routing stable, but the Kafka message key still changes.
- `REPLICA IDENTITY NOTHING`: empty.

If the message key matters, use `DEFAULT`, or `USING INDEX` on a stable unique key. The initial data copy reads the primary key from the catalog, so for `FULL`, `NOTHING` and `USING INDEX` tables its keys differ from those of the streamed changes.

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

With `.WithJsonOptions(o => o.WriteMode = JsonWriteMode.Compact)`, `"k"` and `"r"` are arrays of values without column names. The SQLite, MongoDB, DynamoDB and ClickHouse adapters always use compact mode. The column names come in a schema property:

```
"s": ["schema.table", ["id", 1, 23], ["name", 0, 1043, 104], ...]
                       // column name, part of the key (1/0), type OID, type modifier (only if present)
```

The schema is not repeated in every message. It comes with the first row of a table after the listener connects, and again after the table changes. After a reconnect it is not always repeated for every table. **Consumers must remember the last schema per table** and decode rows with it, as the SQLite, MongoDB and DynamoDB adapters do. A consumer that starts reading in the middle of a stream must already know the schema, for example from its own store.

Partitioned outputs (Kafka topics with more than one partition, RabbitMQ super streams, and Azure Event Hubs with partition-key columns) spread the rows of a table across partitions, so the schema lands only in the partition of the row it comes with. Consumers must therefore share the last schema per table across all partitions, for example in a common store.

## 🔁 Delivery and Deduplication

Delivery is **at-least-once**. A batch of changes is confirmed to PostgreSQL only after the output has acknowledged it. After a failure or a restart, PostgreSQL replays everything that was not confirmed, and those changes are published again, in their original order.

A replay is usually one batch, but it can be more:

- A transaction that was interrupted part-way is replayed from its start.
- The position of a replication slot is persisted only at checkpoints, so after a PostgreSQL crash the slot can go back further.

Every message carries a deduplication key: the transaction final LSN (`"w"`) plus the message number within that transaction (`"n"`). Consumers use it in one of two ways:

- **Apply changes idempotently.** Upsert and delete by key. Replayed changes arrive in order, so the result is the same.
- **Keep a watermark.** Remember the highest `(w, n)` pair processed and skip anything at or below it. Keep one watermark per table, in each partition or stream you read: the order is guaranteed only there. On Kafka, a message the broker rejects can arrive late and out of order; see the Kafka output for how to prevent that.

The rows of the initial data copy have change type `"C"` and no position: `"w"` and `"n"` are 0. Upsert them by key and leave them out of the watermark. A copy that failed and started again publishes its rows again. While a table is being copied and synced, its changes are published behind the changes of the other tables, which is one more reason to keep the watermark per table.

The outputs that store data instead of forwarding messages (SQLite, MongoDB, DynamoDB) save their position with the data, and the changes replayed after a reconnect are skipped for them. `.WithDeduplication(false)` turns that off; it has no effect on the other outputs. The ClickHouse output saves no position: every row it writes carries the position as its version, so a replayed change changes nothing.

## ⚠️ Development Status

**Still early days** — the library is under active development, but it's **fully usable for testing and early integration**.

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

PgOutput2Json supports copying modified PostgreSQL rows to SQLite. By default, rows are copied only when they change, using logical replication and compact JSON messages. 
Optionally, initial data copy can be enabled with `WithInitialDataCopy(true)` when configuring the builder.

The PgOutput2Json library will create the SQLite database if it does not already exist, along with any table included in logical replication. A table is created the first time a row belonging to that table is changed. If a table already exists, it is not modified, but new columns will be created automatically, if a new column is added to the source PostgreSQL table.

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
                options.ConnectionStringBuilder.DataSource = "my_database.s3db";
            }) 
            .Build();  

        await pgOutput2Json.StartAsync(stoppingToken);  
    }  
}
```

## 8. Using MongoDB

PgOutput2Json supports copying modified PostgreSQL rows to MongoDB collections. By default, rows are copied only when they change, using logical replication and compact JSON messages.
Optionally, initial data copy can be enabled with `WithInitialDataCopy(true)` when configuring the builder.

The PgOutput2Json library will create the MongoDB database if it does not already exist, along with one collection for each table included in logical replication. Collections are created the first time a row belonging to the respective table is changed. Additionally, one unique index is created for each collection, covering the fields that are a part of the primary key.

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

## 10. Using Amazon DynamoDB

PgOutput2Json supports copying modified PostgreSQL rows to Amazon DynamoDB tables. By default, rows are copied only when they change, using logical replication and compact JSON messages.
Optionally, initial data copy can be enabled with `WithInitialDataCopy(true)` when configuring the builder.

The PgOutput2Json library will create DynamoDB tables for each table included in logical replication. 

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

Event Hubs uses the partition key (derived from table name) to distribute messages across partitions. This ensures:

- Messages from the same table are processed in order
- Load is distributed across multiple partitions for better throughput
- Parallel processing capabilities for consumers

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

## 13. Using ClickHouse

PgOutput2Json can keep a copy of your PostgreSQL tables in ClickHouse. By default, rows are copied only when they change, using logical replication and compact JSON messages.
Optionally, initial data copy can be enabled with `WithInitialDataCopy(true)` when configuring the builder.

The output uses the HTTP interface of ClickHouse and needs no ClickHouse client library. It is tested with ClickHouse 26.10.

> ⚠️ **Important:** Be sure to set up the PostgreSQL database first, as described in the **QuickStart** section above.

### How the rows are stored

A table is created the first time one of its rows arrives, as a `ReplacingMergeTree` ordered by the key columns of the PostgreSQL table.

ClickHouse is slow at updating and deleting rows, so the output never does either. Every change is inserted as a new row, with two extra columns:

- `_version`: the position of the change in the PostgreSQL WAL (`w * 2^64 + n` of the [deduplication key](#-delivery-and-deduplication)). A copied row has version 0.
- `_deleted`: 1 if the change is a delete.

ClickHouse keeps the row with the highest version per key when it merges the parts of a table, which happens in the background at a time you cannot predict. Until then the older versions are still there, so **read the tables with `FINAL`**:

```sql
SELECT * FROM orders FINAL
```

This returns the current rows, without the deleted ones.

A change that PostgreSQL replays after a reconnect has the same key and version as the first time, so it changes nothing. This is why the output does not save its position.

The row of a delete stays in the table as a marker. ClickHouse can remove the markers: see `OPTIMIZE ... FINAL CLEANUP` in the ClickHouse documentation of `ReplacingMergeTree`.

### Tables

- A table of the `public` schema keeps its name. A table of another schema is named `schema_table`. Set `options.GetTableName` to choose the names yourself.
- A table that already exists is used as it is. To choose the engine (for example a replicated one), the partitioning or the column types, create the table before the first row arrives. It needs the columns `_version UInt128` and `_deleted UInt8`.
- A column added in PostgreSQL is added in ClickHouse. A column dropped in PostgreSQL stays. A changed column type or a changed key is not followed: change the ClickHouse table yourself.
- The database must exist.

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
| `timestamp` | `DateTime64(6)` |
| `timestamptz` | `DateTime64(6, 'UTC')` |
| arrays | `String` with the JSON text, e.g. `[1,2,null]` |
| `json`, `jsonb` | `String` with the JSON text |
| `bytea` | `String` with the bytes in hex |
| anything else (`text`, `time`, `interval`, enums...) | `String` with the text PostgreSQL sends |

Key columns are not `Nullable`, all the other columns are.

### Limits

- **Unchanged large values (TOAST).** PostgreSQL does not send a large value that an update did not change, and the new row in ClickHouse needs it. The output reads it from ClickHouse, with one query per such update. Many updates of rows with large values are slow.
- **`REPLICA IDENTITY FULL`.** Every column is part of the key then, so two rows with the same values in every column are one row in ClickHouse.
- **Tables without a key.** PostgreSQL publishes only their inserts. They are created as a plain `MergeTree`: read them without `FINAL`, and expect a row twice if its insert is replayed after a reconnect.
- **Numbers.** A `numeric` without precision has room for 38 digits before the decimal point. A larger number is rejected by ClickHouse.
- **Dates.** `infinity`, dates before Christ and years above 9999 do not exist in ClickHouse. They are stored as the first or the last date ClickHouse has (year 1 or 9999; ClickHouse 25 and older: 1900 or 2299).
- **Rejected rows.** A row that ClickHouse rejects stops the replication, as with every output: nothing is skipped. Fix the ClickHouse table (for example, change the column to `String`) and the listener continues at its next attempt.

### Batch size

ClickHouse wants few, large inserts. `UseClickHouse` therefore sets the batch size to 10,000 changes (the default of the library is 100). In our test the initial copy ran at about 1,600 rows per second with batches of 100 and at about 75,000 with batches of 10,000. To use another size, call `WithBatchSize` after `UseClickHouse`. A batch that is not full is still sent after the batch wait time (100 ms by default).

With `options.UseAsyncInserts` (on by default) ClickHouse collects the rows of several inserts before it writes them, which keeps the number of parts low when changes arrive a few at a time. An insert is still acknowledged only when the rows are written. It adds some 50 to 200 ms to every batch. Turn it off if you want the lowest latency and your batches are large.

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
| `UseAsyncInserts` | `true` | See [Batch size](#batch-size). |
| `Settings` | empty | ClickHouse settings sent with every request, e.g. `options.Settings["insert_quorum"] = "2"`. |
| `GetTableName` | see [Tables](#tables) | Returns the ClickHouse table name for a PostgreSQL table (`schema.table`). |
| `HttpClient` | none | Your own `HttpClient`, e.g. with a proxy or a client certificate. |
