namespace PgOutput2Json
{
    internal class PgColumnInfo
    {
        public required string ColumnName { get; set; }
        public bool IsKey { get; set; }
        public uint DataTypeId { get; set; }
        public int TypeModifier { get; set; }
    }
}
