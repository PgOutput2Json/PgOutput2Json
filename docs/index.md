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
- ✅ **Kafka**
- ✅ **RabbitMQ** (Streams + Classic Queues)
- ✅ **Redis** (Streams + Pub/Sub Channels)
- ✅ **SQLite** (used by [PgFreshCache](https://github.com/PgOutput2Json/PgFreshCache))
- ✅ **MongoDB**

Plug-and-play adapters handle the heavy lifting — or handle messages directly in your app for maximum control.

## 🧠 How It Works

PostgreSQL 10+ ships with a built-in logical decoding plugin called `pgoutput`, which emits row-level changes (INSERT/UPDATE/DELETE) over a replication slot.

`PgOutput2Json`:

1. Connects to a replication slot via `pgoutput`
2. Converts change events into clean JSON
3. Sends them to your application or a supported message broker

No extra plugins needed — `pgoutput` comes with PostgreSQL by default.

The change events JSON format:
```
{
  "c": "U",             // Change type: I (insert), U (update), D (delete)
  "w": 2485645760,      // WAL end offset
  "t": "schema.table",  // Table name (if enabled in JSON options)
  "k": { ... },         // Key values — included for deletes, and for updates if the key changed,
                        // or old row values, if the table uses REPLICA IDENTITY FULL
  "r": { ... }          // New row values (not present for deletes)
}
```

## ⚠️ Development Status

**Still early days** — the library is under active development, but it's **fully usable for testing and early integration**.

## 1. Quick Start

### 1.1 Configure `postgresql.conf`

To enable logical replication, add the following setting in your `postgresql.conf`:

```
wal_level = logical
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

        await pgOutput2Json.Start(stoppingToken);  
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

        await pgOutput2Json.Start(stoppingToken);  
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

        await pgOutput2Json.Start(stoppingToken);  
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

        await pgOutput2Json.Start(stoppingToken);  
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

        await pgOutput2Json.Start(stoppingToken);  
    }  
}
```

JSON messages will be published to the specified Redis stream. If a stream name suffix is specified, the stream or channel name becomes dynamic, using the format: `stream_name:schema.table:partition`.

> **Note:** The table name is always qualified with the schema using the `.` character.

## 7. Using SQLite

PgOutput2Json supports copying modified PostgreSQL rows to SQLite. My default, rows are copied only when they change, using logical replication and compact JSON messages. 
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

        await pgOutput2Json.Start(stoppingToken);  
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
using PgOutput2Json.MongoDb;

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

        await pgOutput2Json.Start(stoppingToken);  
    }  
}
```
