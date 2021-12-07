namespace PgOutput2Json.Core
{
    public class KeyColumn
    {
        public string ColumnName { get; private set; }
        public int PartitionCount { get; private set; }

        public KeyColumn(string columnName, int partitionCount)
        {
            ColumnName = columnName;
            PartitionCount = partitionCount;
        }
    }
}
