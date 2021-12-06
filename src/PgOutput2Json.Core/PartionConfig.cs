namespace PgOutput2Json.Core
{
    public class PartionConfig
    {
        public string ColumnName { get; private set; }
        public int PartitionCount { get; private set; }

        public PartionConfig(string columnName, int partitionCount)
        {
            ColumnName = columnName;
            PartitionCount = partitionCount;
        }
    }
}
