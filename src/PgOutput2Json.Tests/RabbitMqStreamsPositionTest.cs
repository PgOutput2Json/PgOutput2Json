using System.Buffers;
using System.Text;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using PgOutput2Json.RabbitMqStreams;

namespace PgOutput2Json.Tests
{
    /// <summary>
    /// The position that deduplication reads from the last message of a stream: the "w" and "n" of a message of the
    /// listener, null for anything else.
    /// </summary>
    [TestClass]
    public class RabbitMqStreamsPositionTest
    {
        private static (ulong, ulong)? Read(string json) => RabbitMqStreamsPublisher.ReadPosition(new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(json)));

        [TestMethod]
        [DataRow("{\"c\":\"I\",\"w\":2485645760,\"n\":3,\"t\":\"public.t\",\"r\":{\"id\":1}}", 2485645760UL, 3UL)]
        [DataRow("{\"c\":\"M\",\"w\":18446744073709551615,\"n\":1,\"p\":\"x\"}", ulong.MaxValue, 1UL)]
        [DataRow("{\"c\":\"C\",\"w\":0,\"n\":0,\"r\":{\"w\":5}}", 0UL, 0UL)]
        public void Should_read_w_and_n(string json, ulong w, ulong n)
        {
            Assert.AreEqual((w, n), Read(json));
        }

        [TestMethod]
        [DataRow("{\"c\":\"I\",\"w\":-1,\"n\":1}")]
        [DataRow("{\"c\":\"I\",\"w\":1,\"n\":1.5}")]
        [DataRow("{\"c\":\"I\",\"w\":18446744073709551616,\"n\":1}")]
        [DataRow("{\"c\":\"I\",\"w\":\"1\",\"n\":1}")]
        [DataRow("{\"c\":\"I\",\"w\":1}")]
        [DataRow("{\"r\":{\"w\":1,\"n\":1}}")]
        [DataRow("[1,2]")]
        [DataRow("not json")]
        [DataRow("")]
        public void Should_be_null_for_another_message(string json)
        {
            Assert.IsNull(Read(json));
        }
    }
}
