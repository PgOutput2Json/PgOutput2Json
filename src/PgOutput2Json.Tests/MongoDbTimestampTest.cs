using System;

using Microsoft.VisualStudio.TestTools.UnitTesting;
using MongoDB.Bson;

using PgOutput2Json.MongoDb;

namespace PgOutput2Json.Tests
{
    [TestClass]
    public class MongoDbTimestampTest
    {
        private static readonly TimeZoneInfo Zagreb = TimeZoneInfo.FindSystemTimeZoneById("Europe/Zagreb");

        private static string Utc(BsonValue value)
        {
            Assert.IsInstanceOfType<BsonDateTime>(value);

            return value.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
        }

        /// <summary>
        /// By default a timestamp (without time zone) is stored as its wall time in UTC, never shifted by the machine's
        /// offset (the CI runs this with TZ=Europe/Zagreb); a timestamptz at its own offset.
        /// </summary>
        [TestMethod]
        [DataRow("2026-01-02 03:04:05.123456", false, "2026-01-02T03:04:05.123Z")]
        [DataRow("2026-06-30 23:59:59", false, "2026-06-30T23:59:59.000Z")]
        [DataRow("1900-03-04 05:06:07.5", false, "1900-03-04T05:06:07.500Z")]
        [DataRow("2026-01-02 03:04:05.123456+00", true, "2026-01-02T03:04:05.123Z")]
        [DataRow("2026-01-02 03:04:05+01", true, "2026-01-02T02:04:05.000Z")]
        [DataRow("2026-06-30 23:59:59-05:30", true, "2026-07-01T05:29:59.000Z")]
        public void ConvertTimestamp_should_never_use_the_local_offset(string text, bool withTimeZone, string expected)
        {
            Assert.AreEqual(expected, Utc(MongoDbExtensions.ConvertTimestamp(text, !withTimeZone, TimeZoneInfo.Utc)));
        }

        /// <summary>
        /// With TimestampTimeZone a timestamp (without time zone) is a wall time of that zone, converted as PostgreSQL's
        /// <c>AT TIME ZONE 'Europe/Zagreb'</c> does (the expected values are from PostgreSQL 16): a time skipped when the
        /// clock is set forward gets the offset of before, a time that is there twice the offset of after.
        /// </summary>
        [TestMethod]
        [DataRow("2026-01-02 03:04:05.123456", "2026-01-02T02:04:05.123Z")]
        [DataRow("2026-06-30 23:59:59", "2026-06-30T21:59:59.000Z")]
        [DataRow("2026-03-29 02:30:00", "2026-03-29T01:30:00.000Z")]
        [DataRow("2026-10-25 01:59:59", "2026-10-24T23:59:59.000Z")]
        [DataRow("2026-10-25 02:30:00", "2026-10-25T01:30:00.000Z")]
        [DataRow("2026-10-25 03:00:00", "2026-10-25T02:00:00.000Z")]
        public void ConvertTimestamp_should_take_a_timestamp_as_a_time_of_the_time_zone(string text, string expected)
        {
            Assert.AreEqual(expected, Utc(MongoDbExtensions.ConvertTimestamp(text, true, Zagreb)));
        }

        [TestMethod]
        public void ConvertTimestamp_should_not_apply_the_time_zone_to_a_timestamptz()
        {
            Assert.AreEqual("2026-01-02T03:04:05.000Z", Utc(MongoDbExtensions.ConvertTimestamp("2026-01-02 03:04:05+00", false, Zagreb)));
        }

        [TestMethod]
        public void ConvertTimestamp_should_round_down_before_1970_and_hold_the_first_day_of_year_1()
        {
            Assert.AreEqual("1969-12-31T23:59:59.998Z", Utc(MongoDbExtensions.ConvertTimestamp("1969-12-31 23:59:59.9985", true, TimeZoneInfo.Utc)));

            // before year 1 in UTC: a BSON date, though .NET cannot show it
            Assert.IsInstanceOfType<BsonDateTime>(MongoDbExtensions.ConvertTimestamp("0001-01-01 00:30:00", true, Zagreb));
        }

        /// <summary>
        /// A value a BSON date cannot hold here is stored as PostgreSQL's text instead of stopping the replication.
        /// </summary>
        [TestMethod]
        [DataRow("infinity", false)]
        [DataRow("-infinity", false)]
        [DataRow("infinity", true)]
        [DataRow("0044-03-15 12:00:00 BC", false)]
        [DataRow("0044-03-15 12:00:00+00 BC", true)]
        [DataRow("10000-01-01 00:00:00", false)]
        public void ConvertTimestamp_should_store_the_text_of_a_value_it_cannot_hold(string text, bool withTimeZone)
        {
            Assert.AreEqual(new BsonString(text), MongoDbExtensions.ConvertTimestamp(text, !withTimeZone, Zagreb));
        }
    }
}
