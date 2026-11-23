---
title: Delivery and deduplication
description: Delivery is at-least-once. How replayed changes reach consumers, and how to deduplicate them with the w and n key.
---

Delivery is **at-least-once**. A batch of changes is confirmed to PostgreSQL only after the output has acknowledged it. After a failure or a restart, PostgreSQL replays everything that was not confirmed, and those changes are published again, in their original order.

A replay is usually one batch, but it can be more:

- A transaction that was interrupted part-way is replayed from its start.
- The position of a replication slot is persisted only at checkpoints, so after a PostgreSQL crash the slot can go back further.

Every message carries a deduplication key: the transaction final LSN (`"w"`) plus the message number within that transaction (`"n"`). Consumers use it in one of two ways:

- **Apply changes idempotently.** Upsert and delete by key. Replayed changes arrive in order, so the result is the same.
- **Keep a watermark.** Remember the highest `(w, n)` pair processed and skip anything at or below it. Keep one watermark per table, in each partition or stream you read: the order is guaranteed only there. On Kafka and Kinesis, a message the output rejects can arrive late and out of order; see those outputs.

The rows of the [initial data copy](../configuration/#initial-data-copy) have change type `"C"` and no position: `"w"` and `"n"` are 0. Upsert them by key and leave them out of the watermark. A copy that failed and started again publishes its rows again. While a table is being copied and synced, its changes are published behind the changes of the other tables, which is one more reason to keep the watermark per table.

The outputs that store data instead of forwarding messages (SQLite, MongoDB, DynamoDB) save their position with the data, and the changes replayed after a reconnect are skipped for them. They keep it per subscription, under `options.SubscriptionName`, which they require: give every source database and slot that writes into the same store its own name. A store of an earlier version has one position, which the first subscription takes. If a source database is restored from a backup or replaced, its positions start again lower, and the saved position would skip its changes: stop the listener, delete the position (the key `Position:<subscription name>` in `__pg2j_config`: a row in SQLite, a document in MongoDB, an item in DynamoDB), and start it again. Its changes are then written again from where the slot is. The message outputs publish them again, except a single RabbitMQ stream with [`UseDeduplication`](../outputs/rabbitmq-streams/#deduplication). The ClickHouse output saves no position: every row it writes carries the position as its version, so a replayed change changes nothing.
