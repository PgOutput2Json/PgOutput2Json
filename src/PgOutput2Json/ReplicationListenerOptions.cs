using System.Collections.Generic;

namespace PgOutput2Json
{
    internal class ReplicationListenerOptions
    {
        public string ConnectionString { get; set; }
        public string[] PublicationNames { get; set; }
        public string ReplicationSlotName { get; set; }
        public Dictionary<string, int> TablePartitions { get; private set; } 
        public Dictionary<string, IReadOnlyList<string>> IncludedColumns { get; private set; }
        public PartitionFilter? PartitionFilter { get; private set; }

        public ReplicationListenerOptions(string connectionString,
                                          string replicationSlotName,
                                          string[] publicationNames,
                                          IReadOnlyDictionary<string, int> tablePartitions,
                                          IReadOnlyDictionary<string, IReadOnlyList<string>> includedColumns, 
                                          PartitionFilter? partitionFilter = null)
        {
            ConnectionString = connectionString;
            PublicationNames = publicationNames;
            ReplicationSlotName = replicationSlotName;
            TablePartitions = new Dictionary<string, int>(tablePartitions);
            IncludedColumns = new Dictionary<string, IReadOnlyList<string>>(includedColumns);
            PartitionFilter = partitionFilter;
        }
    }
}
