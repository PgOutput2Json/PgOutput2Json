---
title: ClickHouse
description: Keep a copy of your PostgreSQL tables in ClickHouse with PgOutput2Json.ClickHouse.
---

PgOutput2Json can keep a copy of your PostgreSQL tables in ClickHouse. By default, rows are copied only when they change, using logical replication.
Optionally, initial data copy can be enabled with `WithInitialDataCopy(true)` when configuring the builder.

The output uses the HTTP interface of ClickHouse and needs no ClickHouse client library. It is tested with ClickHouse 26.10.

> **Important:** First set up PostgreSQL as described in [Getting started](../../getting-started/).

## How the rows are stored

A table is created the first time one of its rows arrives, as a `ReplacingMergeTree` ordered by the key columns of the PostgreSQL table.

ClickHouse is slow at updating and deleting rows, so the output never does either. Every change is inserted as a new row, with three extra columns:

- `_version`: the position of the change in the PostgreSQL WAL (`w * 2^64 + n` of the [deduplication key](../../deduplication/)). A copied row has version 0.
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

## Deleted rows

The row of a delete stays in the table, marked deleted. `FINAL` hides it, but it takes space. With `options.CleanUpDeletedRowsAfter = TimeSpan.FromHours(1)` the output sets the cleanup settings of `ReplacingMergeTree` on its tables (a new table when it is created, an existing one on the next start), and ClickHouse removes the deleted rows in a merge once no new rows have arrived in a table for that long. ClickHouse still marks this feature experimental.

- A table that gets new rows all the time never waits that long. Run `OPTIMIZE TABLE orders FINAL CLEANUP` instead, for example at night: the settings allow it. It rewrites the whole table.
- Once a deleted row is removed, nothing hides an older version of it. When PostgreSQL replays changes after a reconnect, a deleted row can show up again until the replayed delete arrives.
- Setting the option back to null does not remove the settings from the tables.

## Tables

- A table of the `public` schema keeps its name. A table of another schema is named `schema_table`. Set `options.GetTableName` to choose the names yourself.
- A table that already exists is used as it is. To choose the column types or an engine the options do not offer (for example a replicated one), create the table before the first row arrives. It needs the columns `_version UInt128` and `_deleted UInt8`.
- A column added in PostgreSQL is added in ClickHouse. A column dropped in PostgreSQL stays. A changed column type is not followed: change the ClickHouse table yourself. Neither is a changed primary key definition (other key columns), because ClickHouse cannot change the order of an existing table: drop the ClickHouse table and copy it again. An update that changes the key value of a row is followed.
- The database must exist.

## Order, partitions and engine

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

## Column types

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

## Arrays

An array is a ClickHouse array, so `has()`, `arrayJoin()` and the other array functions work on it without parsing. Two things differ from PostgreSQL:

- **A null array is an empty array**, because ClickHouse arrays cannot be `Nullable`. A `NULL` element stays `null`.
- **One dimension.** PostgreSQL does not send the number of dimensions of a column (`integer[]` and `integer[][]` are the same type), so the ClickHouse column has one. A multi-dimensional value stops the replication with an error that names the column. Change that column to a `String` (`ALTER TABLE ... MODIFY COLUMN ... String`), and the listener continues with the JSON text (for an array of an enum or a user-defined type, the PostgreSQL array literal).

The library sends the arrays of enums and user-defined types as the PostgreSQL array literal (`{sad,happy}`). The output knows they are arrays from the name of the type that PostgreSQL sends with the schema (an array type is named after its element type with an underscore: `_mood`), and stores their elements as strings. A domain over an array, such as `CREATE DOMAIN ints AS int[]`, is also an `Array(Nullable(String))`.

With `options.UseArrayTypes = false`, new array columns are `String`s with the JSON text, as before version 2.1, and a null array stays `null`. An existing column keeps its type.

## JSON columns

`json` and `jsonb` columns are stored in the `JSON` type of ClickHouse, which needs ClickHouse 25.3 or newer. With `options.UseJsonType = false` they are `String` columns with the JSON text.

The `JSON` type holds only objects, and rejects some values PostgreSQL accepts. The output changes those values, so that no row is rejected:

- A value that is not an object (an array, a number, a string, `true`, a JSON `null`) is stored as `{"v": value}`. ClickHouse does not store a null inside an object, so a JSON `null` becomes `{}`.
- A key that is twice in an object of a `json` column keeps its last value, as in `jsonb`.
- A number too large for a double (above about 1.8e308) is stored as a string.
- Two keys with the same path, such as `"a.b"` and `"b"` inside `"a"`, keep one value.

ClickHouse stores a number with a fraction as a double, and an integer of more than 64 bits as a string.

A `json` column that is part of the key is a `String`, as is an array of `json`.

## Limits

- **Unchanged large values (TOAST).** PostgreSQL does not send a large value that an update did not change, and the new row in ClickHouse needs it. The output reads it from ClickHouse, with one query per such update. Many updates of rows with large values are slow.
- **`REPLICA IDENTITY FULL`.** Every column is part of the key then, so two rows with the same values in every column are one row in ClickHouse.
- **Tables without a key.** PostgreSQL publishes only their inserts. They are created as a plain `MergeTree`: read them without `FINAL`, and expect a row twice if its insert is replayed after a reconnect.
- **Numbers.** A `numeric` without precision has room for 38 digits before the decimal point. A larger number is rejected by ClickHouse.
- **Dates.** `infinity`, dates before Christ and years above 9999 do not exist in ClickHouse. They are stored as the first or the last date ClickHouse has (year 1 or 9999; ClickHouse 25 and older: 1900 or 2299).
- **Rejected rows.** A row that ClickHouse rejects stops the replication, as with every output: nothing is skipped. Fix the ClickHouse table (for example, change the column to `String`) and the listener continues at its next attempt.

## Batch size

ClickHouse wants few, large inserts. `UseClickHouse` therefore sets the batch size to 10,000 changes (the default of the library is 100). In our test the initial copy ran at about 1,600 rows per second with batches of 100 and at about 75,000 with batches of 10,000. To use another size, call `WithBatchSize` after `UseClickHouse`. A batch that is not full is still sent after the batch wait time (100 ms by default).

With `options.UseAsyncInserts` (on by default) ClickHouse collects the rows of several inserts before it writes them, which keeps the number of parts low when changes arrive a few at a time. An insert is still acknowledged only when the rows are written. It adds some 50 to 200 ms to every batch. Turn it off if you want the lowest latency and your batches are large.

## Create a .NET Worker Service

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

## Options

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
