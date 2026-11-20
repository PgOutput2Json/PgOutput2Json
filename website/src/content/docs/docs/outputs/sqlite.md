---
title: SQLite
description: Copy changed PostgreSQL rows to a SQLite database with PgOutput2Json.Sqlite.
---

PgOutput2Json supports copying modified PostgreSQL rows to SQLite. By default, rows are copied only when they change, using logical replication.
Optionally, initial data copy can be enabled with `WithInitialDataCopy(true)` when configuring the builder.

The PgOutput2Json library will create the SQLite database if it does not already exist, along with any table included in logical replication. A table is created the first time a row belonging to that table is changed. If a table already exists, it is not modified, but new columns will be created automatically, if a new column is added to the source PostgreSQL table.

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

> **Important:** First set up PostgreSQL as described in [Getting started](../../getting-started/).

## Dates and times

A `timestamp` (without time zone) is stored as ISO 8601 text without an offset (`2026-01-02T03:04:05.1234560`), so Microsoft.Data.Sqlite and EF Core read it back as the same wall time, with `DateTimeKind.Unspecified`, whatever the time zone of the machine. A `timestamptz` is stored as ISO 8601 text with its offset, in UTC (`2026-01-02T03:04:05.1234560+00:00`). Both columns are created as `DATETIME`. A `date`, `time` or `timetz` is stored as PostgreSQL's text (`2026-01-02`, `03:04:05`, `03:04:05+02`), and a value .NET cannot hold (`infinity`, a date BC) as well.

Earlier versions stored a `timestamp` with the UTC offset of the machine running the output (`+01:00` in Central Europe in January), so it was read back shifted by that offset. Rows already in the database keep that value until they change, or until the table is copied again.

## Batch size

Every batch of changes is one transaction. `UseSqlite` sets the batch size to 1,000 changes (the default of the library is 100). In our [benchmark](https://github.com/PgOutput2Json/PgOutput2Json/tree/main/src/PgOutput2Json.Benchmark) with batches of 1,000 and the database file on the server's disk, the initial copy ran at about 55,000 rows per second and the stream at about 50,000, or about 68,000 and 61,000 with `options.UseWal = true` (the WAL journal mode of SQLite), since a commit then waits less for the disk. With the file in memory (tmpfs), where a commit does not wait for a disk, they ran at about 85,000 and 75,000 (with or without `UseWal`), and at about 72,000 and 68,000 with batches of 100 (measured on a Hetzner CX33, a cost-optimized cloud server with 4 shared vCPUs, 8 GB RAM and its local disk with ext4, with PostgreSQL in Docker on the same machine). Larger batches add little. To use another size, call `WithBatchSize` after `UseSqlite`. A batch that is not full is still written after the batch wait time (100 ms by default).

## Create a .NET Worker Service

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
