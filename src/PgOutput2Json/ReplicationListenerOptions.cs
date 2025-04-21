using System;
using System.Collections.Generic;

namespace PgOutput2Json
{
    public class ReplicationListenerOptions
    {
        public string ConnectionString { get; private set; }
        public string[] PublicationNames { get; private set; }
        public string ReplicationSlotName { get; private set; }

        public bool UseTemporarySlot { get; private set; }
        public TimeSpan IdleFlushTime { get; private set; }
        public int BatchSize { get; private set; }

        public Dictionary<string, int> TablePartitions { get; private set; } 
        public Dictionary<string, IReadOnlyList<string>> IncludedColumns { get; private set; }
        public PartitionFilter? PartitionFilter { get; private set; }

        public ReplicationListenerOptions(string connectionString,
                                          bool useTemporarySlot,
                                          string replicationSlotName,
                                          string[] publicationNames,
                                          TimeSpan idleFlushTime,
                                          int batchSize,
                                          IReadOnlyDictionary<string, int> tablePartitions,
                                          IReadOnlyDictionary<string, IReadOnlyList<string>> includedColumns, 
                                          PartitionFilter? partitionFilter = null)
        {
            ConnectionString = connectionString;
            UseTemporarySlot = useTemporarySlot;
            PublicationNames = publicationNames;
            IdleFlushTime = idleFlushTime;
            BatchSize = batchSize;
            ReplicationSlotName = replicationSlotName;
            TablePartitions = new Dictionary<string, int>(tablePartitions);
            IncludedColumns = new Dictionary<string, IReadOnlyList<string>>(includedColumns);
            PartitionFilter = partitionFilter;
        }
    }
}
