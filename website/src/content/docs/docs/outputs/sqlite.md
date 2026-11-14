---
title: SQLite
description: Copy changed PostgreSQL rows to a SQLite database with PgOutput2Json.Sqlite.
---

PgOutput2Json supports copying modified PostgreSQL rows to SQLite. By default, rows are copied only when they change, using logical replication.
Optionally, initial data copy can be enabled with `WithInitialDataCopy(true)` when configuring the builder.

The PgOutput2Json library will create the SQLite database if it does not already exist, along with any table included in logical replication. A table is created the first time a row belonging to that table is changed. If a table already exists, it is not modified, but new columns will be created automatically, if a new column is added to the source PostgreSQL table.

> **Important:** First set up PostgreSQL as described in [Getting started](../../getting-started/).

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
