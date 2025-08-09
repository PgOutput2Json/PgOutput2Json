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

        public int BatchSize { get; internal set; } = 100;

        /// <summary>
        /// When sending in batches the the sending of a message is delayed for the time specified here (100ms by default, minimum 10ms). 
        /// This allows more messages to join the batch. 
        /// If no new messages are received in the specified time, or the batch is full, the batch is sent. 
        ///
        /// This means that batching increases latency, but improves the overall throughput.
        /// </summary>
        public TimeSpan BatchWaitTime { get; internal set; } = TimeSpan.FromMilliseconds(100);

        public Dictionary<string, int> TablePartitions { get; internal set; } = [];
        public Dictionary<string, IReadOnlyList<string>> IncludedColumns { get; internal set; } = [];
        public Dictionary<string, IReadOnlyList<string>> OrderedKeys { get; internal set; } = [];
        public PartitionFilter? PartitionFilter { get; internal set; }

        /// <summary>
        /// Push the existing data to the publisher. Default is false.
        /// The publisher must support storing the last WAL LSN, because it is used to decide weather to initiate the copy or not.
        /// The copy is initiated only if the last published WAL LSN == 0.
        /// </summary>
        public bool CopyData { get; internal set; }

        /// <summary>
        /// This is only used if initial data copy is enabled AND ordered key columns are provided
        /// </summary>
        public int CopyDataBatchSize { get; internal set; }

        public int MaxParallelCopyJobs { get; internal set; } = 1;
    }
}
