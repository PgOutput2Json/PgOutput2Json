---
title: Deduplication
description: How replayed changes are skipped, with the w and n deduplication key, the startup scan and per-partition watermarks.
---

The current position of a replication slot is persisted only at checkpoints, so after a crash or a reconnect the slot may replay changes that were already consumed. To make replays harmless, every published message carries a deduplication key — the transaction final LSN (`"w"`) plus the message number within that transaction (`"n"`).

Deduplication is enabled by default (disable it with `.WithDeduplication(false)`) and works on two levels:

1. **On connect**, the library asks the publisher for the last durably published position and skips every replayed message at or below it. For partitioned publishers (Kafka, RabbitMQ Streams super streams, and Azure Event Hubs when partition-key columns are configured) the last position of every partition is read, and the **lowest** one is used — only that position is guaranteed to be published to all partitions. With `.WithDeduplication(false)` this startup scan is skipped.
2. **During publishing**, partitioned publishers additionally skip messages already sent to their resolved target partition. The target partition is computed client-side using the same deterministic hash the publisher routes with (Kafka murmur2, RabbitMQ super stream murmur3, Event Hubs murmur2 over the configured partition-key columns), so routing stays stable across restarts. Azure Event Hubs routes client-side only when partition-key columns are set (`WithPartitionKeyFields`); without them it keeps the legacy table-name partition key with service-side routing, and only the listener-level skip applies.

If a partition has no readable history — an empty partition, one emptied by retention, or one that cannot be read during the scan — it contributes a `(0,0)` watermark: messages routed to it are never skipped, and RabbitMQ Streams additionally drops the resume point to the beginning of the stream. The failure mode is therefore always duplicate messages, never silently dropped ones, but consumers should still handle duplicates idempotently (see [Permanent replication slots](../replication-slots/)).

**Amazon Kinesis is the exception:** the Kinesis adapter does not support deduplication at all — Kinesis offers no cheap way to read the last published record of a shard, nor to target a shard client-side, so no watermark can be recovered. On every restart the stream is re-published in full, and consumers must protect themselves by filtering duplicates with the `(w, n)` key, skipping anything at or below the highest pair already processed. See [Amazon Kinesis](../outputs/kinesis/#deduplication).
