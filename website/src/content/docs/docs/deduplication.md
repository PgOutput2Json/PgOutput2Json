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
- **Keep a watermark.** Remember the highest `(w, n)` pair processed and skip anything at or below it. Keep one watermark per table, in each partition or stream you read: the order is guaranteed only there. On Kafka, a message the broker rejects can arrive late and out of order; see the Kafka output for how to prevent that.

The outputs that store data instead of forwarding messages (SQLite, MongoDB, DynamoDB) save their position with the data, and the changes replayed after a reconnect are skipped for them. `.WithDeduplication(false)` turns that off; it has no effect on the other outputs.
