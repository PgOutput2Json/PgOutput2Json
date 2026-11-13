---
title: Getting started
description: Enable logical replication, create a replication user and a publication, and receive the first JSON change events in a .NET worker.
---

## Configure `postgresql.conf`

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

## Create a Replication User

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

## Create a Publication

Now, connect to the target database and create a publication to specify which tables and actions should be replicated:

```sql
CREATE PUBLICATION my_publication
    FOR TABLE my_table1, my_table2
    WITH (publish = 'insert, update, delete');
```

In the example code below, we'll assume the database name is `my_database`.

## Create a .NET Worker Service

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

> **Note:** This example uses a **temporary replication slot**, meaning it won’t capture changes made while the worker was stopped. See [Permanent replication slots](../replication-slots/).

## Next

- [Change events](../change-events/): the JSON format, the message key and compact mode.
- [Outputs](../outputs/kafka/): send the changes to Kafka, RabbitMQ, Redis and others instead of handling them in your application.
- [Configuration](../configuration/): the builder's options.
