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
- **Keep a watermark.** Remember the highest `(w, n)` pair processed and skip anything at or below it. Keep one watermark per table, in each partition or stream you read: the order is guaranteed only there. On Kafka, a message the broker rejects can arrive late and out of order; see the Kafka output for how to prevent that.

The rows of the initial data copy have change type `"C"` and no position: `"w"` and `"n"` are 0. Upsert them by key and leave them out of the watermark. A copy that failed and started again publishes its rows again. While a table is being copied and synced, its changes are published behind the changes of the other tables, which is one more reason to keep the watermark per table.

The outputs that store data instead of forwarding messages (SQLite, MongoDB, DynamoDB) save their position with the data, and the changes replayed after a reconnect are skipped for them. `.WithDeduplication(false)` turns that off; it has no effect on the other outputs. The ClickHouse output saves no position: every row it writes carries the position as its version, so a replayed change changes nothing.

## 🧪 Testing

Every output is covered by integration tests, which run on each change against PostgreSQL 14 to 18.

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

## Using ClickHouse

PgOutput2Json can keep a copy of your PostgreSQL tables in ClickHouse. By default, rows are copied only when they change, using logical replication.
Optionally, initial data copy can be enabled with `WithInitialDataCopy(true)` when configuring the builder.

The output uses the HTTP interface of ClickHouse and needs no ClickHouse client library. It is tested with ClickHouse 26.10.

### How the rows are stored

A table is created the first time one of its rows arrives, as a `ReplacingMergeTree` ordered by the key columns of the PostgreSQL table.

ClickHouse is slow at updating and deleting rows, so the output never does either. Every change is inserted as a new row, with three extra columns:

- `_version`: the position of the change in the PostgreSQL WAL (`w * 2^64 + n` of the [deduplication key](https://pg2json.com/docs/deduplication/)). A copied row has version 0.
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

- A table of the `public` schema keeps its name. A table of another schema is named `schema_table`. Set `options.GetTableName` to choose the names yourself.
- A table that already exists is used as it is. To choose the column types or an engine the options do not offer (for example a replicated one), create the table before the first row arrives. It needs the columns `_version UInt128` and `_deleted UInt8`.
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
| `timestamp` | `DateTime64(6)` |
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

> **Note:** This example uses a **temporary replication slot**, meaning it won’t capture changes made while the worker was stopped.

### Options

| Option | Default | |
|---|---|---|
| `Endpoint` | `http://localhost:8123` | The HTTP interface of ClickHouse. |
| `Database` | `default` | The database of the tables. It must exist. |
| `Username`, `Password` | `default`, empty | |
| `UseAsyncInserts` | `true` | See [Batch size](#batch-size). |
| `UseJsonType` | `true` | See [JSON columns](#json-columns). |
| `UseArrayTypes` | `true` | See [Arrays](#arrays). |
| `CleanUpDeletedRowsAfter` | none | Removes the deleted rows. See [Deleted rows](#deleted-rows). |
| `Tables` | empty | The order, partitions and engine per table. See [Order, partitions and engine](#order-partitions-and-engine). |
| `Settings` | empty | ClickHouse settings sent with every request, e.g. `options.Settings["insert_quorum"] = "2"`. |
| `GetTableName` | see [Tables](#tables) | Returns the ClickHouse table name for a PostgreSQL table (`schema.table`). |
| `HttpClient` | none | Your own `HttpClient`, e.g. with a proxy or a client certificate. |

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
