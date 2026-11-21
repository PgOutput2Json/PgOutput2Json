using System;

using Microsoft.VisualStudio.TestTools.UnitTesting;
using MongoDB.Bson;

using PgOutput2Json.MongoDb;

namespace PgOutput2Json.Tests
{
    [TestClass]
    public class MongoDbTimestampTest
    {
        /// <summary>
        /// A timestamp (without time zone) is stored as its wall time in UTC, never shifted by the machine's offset
        /// (the CI runs this with TZ=Europe/Zagreb); a timestamptz at its own offset.
        /// </summary>
        [TestMethod]
        [DataRow("2026-01-02 03:04:05.123456", "2026-01-02T03:04:05.123Z")]
        [DataRow("2026-06-30 23:59:59", "2026-06-30T23:59:59.000Z")]
        [DataRow("1900-03-04 05:06:07.5", "1900-03-04T05:06:07.500Z")]
        [DataRow("2026-01-02 03:04:05.123456+00", "2026-01-02T03:04:05.123Z")]
        [DataRow("2026-01-02 03:04:05+01", "2026-01-02T02:04:05.000Z")]
        [DataRow("2026-06-30 23:59:59-05:30", "2026-07-01T05:29:59.000Z")]
        public void ConvertTimestamp_should_never_use_the_local_offset(string text, string expected)
        {
            var value = MongoDbExtensions.ConvertTimestamp(text);

            Assert.IsInstanceOfType<BsonDateTime>(value);
            Assert.AreEqual(expected, value.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ"));
        }

        /// <summary>
        /// A value a BSON date cannot hold here is stored as PostgreSQL's text instead of stopping the replication.
        /// </summary>
        [TestMethod]
        [DataRow("infinity")]
        [DataRow("-infinity")]
        [DataRow("0044-03-15 12:00:00 BC")]
        [DataRow("0044-03-15 12:00:00+00 BC")]
        [DataRow("10000-01-01 00:00:00")]
        public void ConvertTimestamp_should_store_the_text_of_a_value_it_cannot_hold(string text)
        {
            Assert.AreEqual(new BsonString(text), MongoDbExtensions.ConvertTimestamp(text));
        }
    }
}
