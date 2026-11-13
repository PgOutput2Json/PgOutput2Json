using System;
using System.Collections.Generic;

using Npgsql;

namespace PgOutput2Json
{
    public sealed class ReplicationListenerOptions
    {
        public NpgsqlDataSourceBuilder DataSourceBuilder { get; private set; } = new NpgsqlDataSourceBuilder();

        public string ConnectionString => DataSourceBuilder.ConnectionString;

        public string[] PublicationNames { get; internal set; } = [];
        public string ReplicationSlotName { get; internal set; } = string.Empty;

        public bool UseTemporarySlot { get; internal set; } = true;

        /// <summary>
        /// Used only by the publishers that save their position with the data (SQLite, MongoDB, DynamoDB):
        /// changes replayed by PostgreSQL after a reconnect are skipped. All other publishers deliver
        /// at-least-once, and consumers deduplicate on the "w" and "n" fields. Default is true.
        /// </summary>
        public bool UseDeduplication { get; set; } = true;

        public int BatchSize { get; internal set; } = 100;

        /// <summary>
        /// When sending in batches the the sending of a message is delayed for the time specified here (100ms by default, minimum 10ms). 
        /// This allows more messages to join the batch. 
        /// If no new messages are received in the specified time, or the batch is full, the batch is sent. 
        ///
        /// This means that batching increases latency, but improves the overall throughput.
        /// </summary>
        public TimeSpan BatchWaitTime { get; internal set; } = TimeSpan.FromMilliseconds(100);

        /// <summary>
        /// This is used to extract partition key values needed for some publishers (Kafka, RabbitMq).
        /// It is an optimization feature - to avoid parsing json during publishing.
        /// </summary>
        public Dictionary<string, IReadOnlyList<string>> PartitionKeyColumns { get; internal set; } = [];

        public Dictionary<string, IReadOnlyList<string>> IncludedColumns { get; internal set; } = [];

        /// <summary>
        /// Copy the existing rows of the published tables to the publisher. Default is false.
        /// Each table is copied once per replication slot, from a snapshot, while the other tables are streamed;
        /// the table's own changes are published after its copy. The progress is kept in the source database.
        /// </summary>
        public bool CopyData { get; internal set; }

        /// <summary>
        /// The number of tables synced at the same time. Each one uses a replication slot and two connections.
        /// </summary>
        public int MaxParallelCopyJobs { get; internal set; } = 2;

        /// <summary>
        /// Interval at which pg_logical_emit_message is called to prevent restart_lsn from lagging
        /// when replicated tables are idle but other tables generate WAL.
        /// Set to <see cref="TimeSpan.Zero"/> or <see cref="System.Threading.Timeout.InfiniteTimeSpan"/> to disable.
        /// Default is 10 seconds.
        /// </summary>
        public TimeSpan IdleWalMessageInterval { get; internal set; } = TimeSpan.FromSeconds(10);
    }
}
