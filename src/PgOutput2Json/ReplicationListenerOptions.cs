using System.Collections.Generic;

namespace PgOutput2Json
{
    internal class ReplicationListenerOptions
    {
        public string ConnectionString { get; set; }
        public string[] PublicationNames { get; set; }
        public string ReplicationSlotName { get; set; }
        public Dictionary<string, KeyColumn> KeyColumns { get; private set; } 
        public Dictionary<string, IReadOnlyList<string>> IncludedColumns { get; private set; }
        public PartitionFilter? PartitionFilter { get; private set; }

        public ReplicationListenerOptions(string connectionString,
                                          string replicationSlotName,
                                          string[] publicationNames,
                                          IReadOnlyDictionary<string, KeyColumn> keyColumns,
                                          IReadOnlyDictionary<string, IReadOnlyList<string>> includedColumns, 
                                          PartitionFilter? partitionFilter = null)
        {
            ConnectionString = connectionString;
            PublicationNames = publicationNames;
            ReplicationSlotName = replicationSlotName;
            KeyColumns = new Dictionary<string, KeyColumn>(keyColumns);
            IncludedColumns = new Dictionary<string, IReadOnlyList<string>>(includedColumns);
            PartitionFilter = partitionFilter;
        }
    }
}
