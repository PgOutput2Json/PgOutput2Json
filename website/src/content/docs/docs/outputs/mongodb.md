---
title: MongoDB
description: Copy changed PostgreSQL rows to MongoDB collections with PgOutput2Json.MongoDb.
---

PgOutput2Json supports copying modified PostgreSQL rows to MongoDB collections. By default, rows are copied only when they change, using logical replication.
Optionally, initial data copy can be enabled with `WithInitialDataCopy(true)` when configuring the builder.

The PgOutput2Json library will create the MongoDB database if it does not already exist, along with one collection for each table included in logical replication. Collections are created the first time a row belonging to the respective table is changed. Additionally, one unique index is created for each collection, covering the fields that are a part of the primary key. A key field that is an array is stored as its JSON text (`["a","b"]`): a unique index on an array field indexes each element, so two rows whose keys share an element would be duplicates.

Arrays are stored as BSON arrays. Arrays of enums, user-defined types and domains over arrays are arrays of strings; before version 2.1 they were a string with the PostgreSQL array literal (`{sad,happy}`), and documents written before keep it until their row changes.

A `timestamp` (without time zone) has no time zone in PostgreSQL, but a BSON date is always a moment in UTC. By default the wall time is stored as if it were UTC (`2026-01-02 03:04:05` as `2026-01-02T03:04:05Z`), so the MongoDB driver reads it back as the same wall time, whatever the time zone of the machine running the output: the `Z` only stands for "no time zone". If your timestamps are times of a known zone, set `options.TimestampTimeZone` (for example `TimeZoneInfo.FindSystemTimeZoneById("Europe/Zagreb")`), and they are stored as the moments they are (`2026-01-02T02:04:05Z`), converted as PostgreSQL's `AT TIME ZONE` does: a time skipped when the clock is set forward gets the offset of before, and a time that is there twice the offset of after. A `timestamptz` is stored at its own moment, whatever the option. BSON dates have milliseconds, so the microseconds are cut off. A value a BSON date cannot hold here (`infinity`, a date BC, a year after 9999) is stored as PostgreSQL's text, and a null in an array of timestamps as null. Earlier versions stored a `timestamp` shifted by the UTC offset of the machine running the output (an hour earlier in Central Europe in January), a null in an array of timestamps as 1970-01-01, and stopped the replication on a value they could not read. Documents already written keep their values until their row changes, or until the table is copied again.

A changed row replaces its document (found by the key), so the document has exactly the columns of the row: a field that is not a column of the table is not kept. An update that did not change a large value (TOAST), which PostgreSQL does not send, sets the other fields instead, and keeps that one. Writing a change to MongoDB costs it a look-up of the key and a write of the document, so its own speed is the limit of this output.

> **Important:** First set up PostgreSQL as described in [Getting started](../../getting-started/).

## Create a .NET Worker Service

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
