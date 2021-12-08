﻿using System.Collections.Generic;

namespace PgOutput2Json
{
    internal class ReplicationListenerOptions
    {
        public string ConnectionString { get; set; }
        public string[] PublicationNames { get; set; }
        public string ReplicationSlotName { get; set; }

        public Dictionary<string, KeyColumn> KeyColumns { get; set; } 
            = new Dictionary<string, KeyColumn>();

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
