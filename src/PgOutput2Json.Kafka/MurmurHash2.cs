using System.Text;

namespace PgOutput2Json.Kafka
{
    /// <summary>
    /// MurmurHash2, compatible with the hash used by the default Kafka partitioner.
    /// Used to select the target partition of a message client-side, so the
    /// per-partition deduplication watermarks can be tracked.
    /// The hash is stable across restarts, unlike string.GetHashCode().
    /// </summary>
    internal static class MurmurHash2
    {
        private const int Seed = unchecked((int)0x9747b28c);

        private const int Multiplier = 0x5bd1e995;

        private const int Shift = 24;

        public static int Hash(string value)
        {
            return Hash(Encoding.UTF8.GetBytes(value));
        }

        public static int Hash(byte[] data)
        {
            var length = data.Length;
            var h = Seed ^ length;
            var blocks = length >> 2;

            for (var i = 0; i < blocks; i++)
            {
                var idx = i << 2;
                var k = data[idx] | (data[idx + 1] << 8) | (data[idx + 2] << 16) | (data[idx + 3] << 24);

                k = unchecked(k * Multiplier);
                k ^= (int)((uint)k >> Shift);
                k = unchecked(k * Multiplier);

                h = unchecked(h * Multiplier);
                h ^= k;
            }

            var tail = blocks << 2;

            switch (length & 3)
            {
                case 3:
                    h ^= data[tail + 2] << 16;
                    goto case 2;
                case 2:
                    h ^= data[tail + 1] << 8;
                    goto case 1;
                case 1:
                    h ^= data[tail];
                    h = unchecked(h * Multiplier);
                    break;
            }

            h ^= (int)((uint)h >> 13);
            h = unchecked(h * Multiplier);
            h ^= (int)((uint)h >> 15);

            return h;
        }
    }
}