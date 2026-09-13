using Microsoft.VisualStudio.TestTools.UnitTesting;

using PgOutput2Json.Kafka;

namespace PgOutput2Json.Tests
{
    [TestClass]
    public class MurmurHash2Test
    {
        // Test vectors for Kafka's standard murmur2 partitioner hash
        // (org.apache.kafka.common.utils.Utils.murmur2).
        [TestMethod]
        [DataRow("21", -973932308)]
        [DataRow("foobar", -790332482)]
        [DataRow("a-little-bit-long-string", -985981536)]
        [DataRow("a-little-bit-longer-string", -1486304829)]
        [DataRow("lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h189hfd", 233120121)]
        public void Hash_should_match_kafka_murmur2(string value, int expected)
        {
            Assert.AreEqual(expected, MurmurHash2.Hash(value));
        }

        [TestMethod]
        public void Hash_should_produce_valid_partition_index()
        {
            var partitionCount = 3;

            for (var i = 0; i < 1000; i++)
            {
                var index = (MurmurHash2.Hash($"test-{i}") & 0x7fffffff) % partitionCount;

                Assert.IsTrue(index >= 0 && index < partitionCount, $"index out of range for test-{i}: {index}");
            }
        }
    }
}