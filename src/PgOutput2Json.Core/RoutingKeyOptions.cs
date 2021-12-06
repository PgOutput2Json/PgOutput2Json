namespace PgOutput2Json.Core
{
    public class RoutingKeyOptions
    {
        public string ColumnName { get; private set; }
        public int PartitionCount { get; private set; }

        public RoutingKeyOptions(string columnName, int partitionCount)
        {
            ColumnName = columnName;
            PartitionCount = partitionCount;
        }
    }
}
