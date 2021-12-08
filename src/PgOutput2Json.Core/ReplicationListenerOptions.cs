namespace PgOutput2Json.Core
{
    internal class ReplicationListenerOptions
    {
        public string ConnectionString { get; set; }
        public string[] PublicationNames { get; set; }
        public string ReplicationSlotName { get; set; }

        public Dictionary<string, KeyColumn> KeyColumns { get; set; } 
            = new Dictionary<string, KeyColumn>();

        /// <summary>
        /// Should nulls be written to JSON output. Default is false.
        /// </summary>
        public bool WriteNulls { get; set; } = false;

        

        public ReplicationListenerOptions(string connectionString,
                                          string replicationSlotName,
                                          params string[] publicationNames)
        {
            ConnectionString = connectionString;
            PublicationNames = publicationNames;
            ReplicationSlotName = replicationSlotName;
        }
    }
}
