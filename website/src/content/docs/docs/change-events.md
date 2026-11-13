---
title: Change events
description: The JSON format of a change event, the message key and its dependence on the replica identity, and compact mode.
---

PostgreSQL 10+ ships with a built-in logical decoding plugin called `pgoutput`, which emits row-level changes (INSERT/UPDATE/DELETE) over a replication slot.

`PgOutput2Json`:

1. Connects to a replication slot via `pgoutput`
2. Converts change events into clean JSON
3. Sends them to your application or a supported message broker

No extra plugins needed — `pgoutput` comes with PostgreSQL by default.

The change events JSON format:

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

## Message key

Besides the JSON, every message has a key: the values of the table's key columns, e.g. `[1]`. Depending on the output, it is the message key (Kafka), the routing or partition key (RabbitMQ, Redis, Kinesis, and Kafka without partition-key columns), or the message id (Azure Event Hubs, RabbitMQ Streams).

The key columns are the **replica identity** columns PostgreSQL sends with the change. The replication protocol does not carry the primary key, and the library does not query the database while streaming, so:

- `REPLICA IDENTITY DEFAULT`: the primary key, stable for all changes to a row.
- `REPLICA IDENTITY USING INDEX`: the index columns.
- `REPLICA IDENTITY FULL`: **all columns**, so the key changes with every update. Kafka log compaction, and routing by key, then treat each version of a row as a different row. Partition-key columns (`WithPartitionKeyFields`) keep the routing stable, but the Kafka message key still changes.
- `REPLICA IDENTITY NOTHING`: empty.

If the message key matters, use `DEFAULT`, or `USING INDEX` on a stable unique key. The initial data copy reads the primary key from the catalog, so for `FULL`, `NOTHING` and `USING INDEX` tables its keys differ from those of the streamed changes.

## Compact mode

With `.WithJsonOptions(o => o.WriteMode = JsonWriteMode.Compact)`, `"k"` and `"r"` are arrays of values without column names. The SQLite, MongoDB and DynamoDB adapters always use compact mode. The column names come in a schema property:

```jsonc
"s": ["schema.table", ["id", 1, 23], ["name", 0, 1043, 104], ...]
                       // column name, part of the key (1/0), type OID, type modifier (only if present)
```

The schema is not repeated in every message. It comes with the first row of a table after the listener connects, and again after the table changes. After a reconnect it is not always repeated for every table. **Consumers must remember the last schema per table** and decode rows with it, as the SQLite, MongoDB and DynamoDB adapters do. A consumer that starts reading in the middle of a stream must already know the schema, for example from its own store.

Partitioned outputs (Kafka topics with more than one partition, RabbitMQ super streams, and Azure Event Hubs with partition-key columns) spread the rows of a table across partitions, so the schema lands only in the partition of the row it comes with. Consumers must therefore share the last schema per table across all partitions, for example in a common store.
