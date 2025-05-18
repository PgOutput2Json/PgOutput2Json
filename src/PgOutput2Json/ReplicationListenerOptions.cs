using System;
using System.Collections.Generic;

using Npgsql;

namespace PgOutput2Json
{
    public sealed class ReplicationListenerOptions
    {
        public NpgsqlDataSourceBuilder DataSourceBuilder { get; private set; }

        public string ConnectionString => DataSourceBuilder.ConnectionString;

        public string[] PublicationNames { get; private set; }
        public string ReplicationSlotName { get; private set; }

        public bool UseTemporarySlot { get; private set; }
        public TimeSpan IdleFlushTime { get; private set; }
        public int BatchSize { get; private set; }

        public Dictionary<string, int> TablePartitions { get; private set; } 
        public Dictionary<string, IReadOnlyList<string>> IncludedColumns { get; private set; }
        public Dictionary<string, IReadOnlyList<string>> OrderedKeys { get; private set; }
        public PartitionFilter? PartitionFilter { get; private set; }

        /// <summary>
        /// Push the existing data to the publisher. Default is false.
        /// The publisher must support storing the last WAL LSN, because it is used to decide weather to initiate the copy or not.
        /// The copy is initiated only if the last published WAL LSN == 0.
        /// </summary>
        public bool CopyData { get; private set; }

        /// <summary>
        /// This is only used if initial data copy is enabled AND ordered key columns are provided
        /// </summary>
        public int CopyDataBatchSize { get; private set; } = 0;

        public int MaxParallelCopyJobs { get; private set; }

        public ReplicationListenerOptions(NpgsqlDataSourceBuilder dataSourceBuilder,
                                          bool useTemporarySlot,
                                          string replicationSlotName,
                                          string[] publicationNames,
                                          TimeSpan idleFlushTime,
                                          int batchSize,
                                          IReadOnlyDictionary<string, int> tablePartitions,
                                          IReadOnlyDictionary<string, IReadOnlyList<string>> includedColumns, 
                                          IReadOnlyDictionary<string, IReadOnlyList<string>> orderedKeys, 
                                          PartitionFilter? partitionFilter = null,
                                          bool copyData = false,
                                          int copyDataBatchSize = 0,
                                          int maxParallelCopyJobs = 1)
        {
            DataSourceBuilder = dataSourceBuilder;
            UseTemporarySlot = useTemporarySlot;
            PublicationNames = publicationNames;
            IdleFlushTime = idleFlushTime;
            BatchSize = batchSize;
            ReplicationSlotName = replicationSlotName;
            TablePartitions = new Dictionary<string, int>(tablePartitions);
            IncludedColumns = new Dictionary<string, IReadOnlyList<string>>(includedColumns);
            OrderedKeys = new Dictionary<string, IReadOnlyList<string>>(orderedKeys);
            PartitionFilter = partitionFilter;
            CopyData = copyData;
            CopyDataBatchSize = copyDataBatchSize;
            MaxParallelCopyJobs = maxParallelCopyJobs;
        }
    }
}
