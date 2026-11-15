using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using PgOutput2Json.ClickHouse;

namespace PgOutput2Json.Tests
{
    [TestClass]
    public class ClickHouseEncodingTest
    {
        /// <summary>
        /// A StringBuilder of several chunks, with a surrogate pair (an emoji) split by the end of the first one.
        /// </summary>
        private static StringBuilder SplitText(out string expected)
        {
            var text = new StringBuilder(4);

            text.Append("abc");
            text.Append("\U0001F600 é \"x\"");
            text.Append(new string('y', 20000));

            expected = text.ToString();

            var chunks = new System.Collections.Generic.List<System.ReadOnlyMemory<char>>();

            foreach (var chunk in text.GetChunks()) chunks.Add(chunk);

            Assert.IsTrue(chunks.Count > 1, "more than one chunk");
            Assert.IsTrue(char.IsHighSurrogate(chunks[0].Span[^1]), "the first chunk ends in the middle of the pair");

            return text;
        }

        [TestMethod]
        public void Utf8_buffer_should_encode_a_pair_split_between_chunks_and_be_reused()
        {
            var text = SplitText(out var expected);

            var buffer = new Utf8Buffer();

            Assert.AreEqual(expected, Encoding.UTF8.GetString(buffer.Encode(text).Span));

            // a shorter text after a longer one: only its own bytes
            Assert.AreEqual("[1]", Encoding.UTF8.GetString(buffer.Encode(new StringBuilder("[1]")).Span));
        }

        [TestMethod]
        public void Utf8_array_should_encode_a_pair_split_between_chunks()
        {
            var text = SplitText(out var expected);

            Assert.AreEqual(expected, Encoding.UTF8.GetString(Utf8Buffer.ToArray(text)));
            Assert.AreEqual(0, Utf8Buffer.ToArray(new StringBuilder()).Length);
        }

        [TestMethod]
        public async Task Content_should_send_a_pair_split_between_chunks()
        {
            var text = SplitText(out var expected);

            using var content = new StringBuilderContent(text);

            Assert.AreEqual(expected, await content.ReadAsStringAsync());
            Assert.AreEqual("utf-8", content.Headers.ContentType!.CharSet);
        }
    }
}
