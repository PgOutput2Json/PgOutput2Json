namespace PgOutput2Json
{
    internal class PartitionFilter
    {
        public int FromInclusive { get; }
        public int ToExclusive { get; }

        public PartitionFilter(int fromInclusive, int toExclusive)
        {
            FromInclusive = fromInclusive;
            ToExclusive = toExclusive;
        }
    }
}
