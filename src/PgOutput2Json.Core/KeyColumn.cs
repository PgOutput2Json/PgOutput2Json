namespace PgOutput2Json.Core
{
    public class KeyColumn
    {
        public string[] ColumnNames { get; private set; }
        public int PartitionCount { get; private set; }

        public KeyColumn(int partitionCount, params string[] columnNames)
        {
            PartitionCount = partitionCount;
            ColumnNames = columnNames;
        }

        public KeyColumn(params string[] columnNames)
            : this(1, columnNames)
        {
        }
    }
}
