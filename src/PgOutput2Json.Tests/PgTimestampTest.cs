using System;
using System.Globalization;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace PgOutput2Json.Tests
{
    [TestClass]
    public class PgTimestampTest
    {
        private static readonly TimeZoneInfo Zagreb = TimeZoneInfo.FindSystemTimeZoneById("Europe/Zagreb");
        private static readonly TimeZoneInfo NewYork = TimeZoneInfo.FindSystemTimeZoneById("America/New_York");
        private static readonly TimeZoneInfo Sydney = TimeZoneInfo.FindSystemTimeZoneById("Australia/Sydney");
        private static readonly TimeZoneInfo Kolkata = TimeZoneInfo.FindSystemTimeZoneById("Asia/Kolkata");

        private static string Format(long ticks) => new DateTime(ticks, DateTimeKind.Unspecified).ToString("yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture);

        private static string Utc(string text, TimeZoneInfo timeZone)
        {
            Assert.IsTrue(PgTimestamp.TryGetUtcTicks(text, timeZone, out var utcTicks), text);

            return Format(utcTicks);
        }

        /// <summary>
        /// The wall time as written, whatever the time zone of the machine (the CI runs the tests also with TZ=Europe/Zagreb).
        /// </summary>
        [TestMethod]
        [DataRow("2026-01-01", "2026-01-01 00:00:00.0000000")]
        [DataRow("2026-01-01 00:00:00", "2026-01-01 00:00:00.0000000")]
        [DataRow("2026-01-02 03:04:05", "2026-01-02 03:04:05.0000000")]
        [DataRow("2026-01-02T03:04:05", "2026-01-02 03:04:05.0000000")]
        [DataRow("2026-12-31 23:59:59", "2026-12-31 23:59:59.0000000")]
        [DataRow("0001-01-01 00:00:00", "0001-01-01 00:00:00.0000000")]
        [DataRow("9999-12-31 23:59:59.999999", "9999-12-31 23:59:59.9999990")]
        [DataRow("2024-02-29 12:00:00", "2024-02-29 12:00:00.0000000")]
        [DataRow("2000-02-29 12:00:00", "2000-02-29 12:00:00.0000000")]
        [DataRow("1969-12-31 23:59:59.999999", "1969-12-31 23:59:59.9999990")]
        [DataRow("1950-06-01 12:00:00.25", "1950-06-01 12:00:00.2500000")]
        [DataRow("1900-01-01 00:00:00", "1900-01-01 00:00:00.0000000")]
        public void TryParse_should_read_the_wall_time_without_a_time_zone(string text, string expected)
        {
            Assert.IsTrue(PgTimestamp.TryParse(text, out var ticks, out var offset));
            Assert.IsNull(offset);
            Assert.AreEqual(expected, Format(ticks));
        }

        /// <summary>
        /// A fraction of a second has one to six digits in PostgreSQL's text; more are cut to microseconds.
        /// </summary>
        [TestMethod]
        [DataRow("2026-01-01 00:00:00.1", "2026-01-01 00:00:00.1000000")]
        [DataRow("2026-01-01 00:00:00.12", "2026-01-01 00:00:00.1200000")]
        [DataRow("2026-01-01 00:00:00.123", "2026-01-01 00:00:00.1230000")]
        [DataRow("2026-01-01 00:00:00.1234", "2026-01-01 00:00:00.1234000")]
        [DataRow("2026-01-01 00:00:00.12345", "2026-01-01 00:00:00.1234500")]
        [DataRow("2026-01-01 00:00:00.123456", "2026-01-01 00:00:00.1234560")]
        [DataRow("2026-01-01 00:00:00.1234567", "2026-01-01 00:00:00.1234560")]
        [DataRow("2026-01-01 00:00:00.123456789", "2026-01-01 00:00:00.1234560")]
        [DataRow("2026-01-01 00:00:00.000001", "2026-01-01 00:00:00.0000010")]
        [DataRow("2026-01-01 00:00:00.999999", "2026-01-01 00:00:00.9999990")]
        [DataRow("2026-01-01 00:00:00.000000", "2026-01-01 00:00:00.0000000")]
        public void TryParse_should_cut_the_fraction_to_microseconds(string text, string expected)
        {
            Assert.IsTrue(PgTimestamp.TryParse(text, out var ticks, out _));
            Assert.AreEqual(expected, Format(ticks));
        }

        /// <summary>
        /// The offset of a timestamptz, in seconds east of UTC: +HH, +HH:MM, +HH:MM:SS (a historical local mean time), or Z.
        /// </summary>
        [TestMethod]
        [DataRow("2026-01-01 00:00:00+00", 0)]
        [DataRow("2026-01-01 00:00:00-00", 0)]
        [DataRow("2026-01-01 00:00:00Z", 0)]
        [DataRow("2026-01-01 00:00:00+01", 3600)]
        [DataRow("2026-01-01 00:00:00-05", -18000)]
        [DataRow("2026-01-01 00:00:00+05:30", 19800)]
        [DataRow("2026-01-01 00:00:00-03:30", -12600)]
        [DataRow("2026-01-01 00:00:00+05:45", 20700)]
        [DataRow("2026-01-01 00:00:00+14", 50400)]
        [DataRow("2026-01-01 00:00:00-12", -43200)]
        [DataRow("1890-01-01 00:00:00+01:03:52", 3832)]
        [DataRow("1890-01-01 00:00:00-00:01:15", -75)]
        [DataRow("2026-01-01 00:00:00.123456+02", 7200)]
        public void TryParse_should_read_the_offset(string text, int expected)
        {
            Assert.IsTrue(PgTimestamp.TryParse(text, out _, out var offset));
            Assert.AreEqual(expected, offset);
        }

        /// <summary>
        /// Anything that is not PostgreSQL's text of a date or a time of the years 1 to 9999 is not read.
        /// </summary>
        [TestMethod]
        [DataRow("")]
        [DataRow(" ")]
        [DataRow("infinity")]
        [DataRow("-infinity")]
        [DataRow("Infinity")]
        [DataRow("NaN")]
        [DataRow("null")]
        [DataRow("0044-03-15 12:00:00 BC")]
        [DataRow("0044-03-15 12:00:00+00 BC")]
        [DataRow("0044-03-15 BC")]
        [DataRow("10000-01-01 00:00:00")]
        [DataRow("294276-12-31 23:59:59")]
        [DataRow("0000-01-01 00:00:00")]
        [DataRow("2026-00-01 00:00:00")]
        [DataRow("2026-13-01 00:00:00")]
        [DataRow("2026-01-00 00:00:00")]
        [DataRow("2026-01-32 00:00:00")]
        [DataRow("2026-02-29 00:00:00")]
        [DataRow("1900-02-29 00:00:00")]
        [DataRow("2026-04-31 00:00:00")]
        [DataRow("2026-01-01 24:00:00")]
        [DataRow("2026-01-01 23:60:00")]
        [DataRow("2026-01-01 23:59:60")]
        [DataRow("2026-01-01 1:00:00")]
        [DataRow("2026-01-01 01:00")]
        [DataRow("2026-01-01 01")]
        [DataRow("2026-01-01 ")]
        [DataRow("2026-01-01 00:00:00.")]
        [DataRow("2026-01-01 00:00:00.12a")]
        [DataRow("2026-01-01 00:00:00 ")]
        [DataRow("2026-01-01 00:00:00+")]
        [DataRow("2026-01-01 00:00:00+1")]
        [DataRow("2026-01-01 00:00:00+01:")]
        [DataRow("2026-01-01 00:00:00+01:6")]
        [DataRow("2026-01-01 00:00:00+01:60")]
        [DataRow("2026-01-01 00:00:00+01:00:60")]
        [DataRow("2026-01-01 00:00:00+01:00:00:00")]
        [DataRow("2026-01-01 00:00:00 +01")]
        [DataRow("2026-01-01 00:00:00ZZ")]
        [DataRow("2026-01-01 00:00:00z")]
        [DataRow("2026-01-01 00:00:00 UTC")]
        [DataRow("2026-01-01 00:00:00 CET")]
        [DataRow("2026-1-01 00:00:00")]
        [DataRow("2026-01-1 00:00:00")]
        [DataRow("26-01-01 00:00:00")]
        [DataRow("2026/01/01 00:00:00")]
        [DataRow("01/01/2026 00:00:00")]
        [DataRow("2026-01-01x00:00:00")]
        [DataRow("Thu Jan 01 00:00:00 2026")]
        [DataRow("１２３４-01-01 00:00:00")]
        public void TryParse_should_not_read_other_text(string text)
        {
            Assert.IsFalse(PgTimestamp.TryParse(text, out _, out _), text);
            Assert.IsFalse(PgTimestamp.TryGetUtcTicks(text, TimeZoneInfo.Utc, out _), text);
        }

        /// <summary>
        /// A timestamptz is at its own offset, whatever the time zone for timestamps.
        /// </summary>
        [TestMethod]
        [DataRow("2026-01-01 00:00:00+00", "2026-01-01 00:00:00.0000000")]
        [DataRow("2026-01-01 00:00:00+01", "2025-12-31 23:00:00.0000000")]
        [DataRow("2026-01-01 00:00:00-05", "2026-01-01 05:00:00.0000000")]
        [DataRow("2026-01-01 00:00:00+05:30", "2025-12-31 18:30:00.0000000")]
        [DataRow("2026-03-29 02:30:00+00", "2026-03-29 02:30:00.0000000")]
        [DataRow("0001-01-01 00:30:00+01", "0000-12-31 23:30:00.0000000")]
        [DataRow("9999-12-31 23:00:00-05", "10000-01-01 04:00:00.0000000")]
        public void TryGetUtcTicks_should_use_the_offset_of_a_timestamptz(string text, string expected)
        {
            if (expected.StartsWith("0000-", StringComparison.Ordinal) || expected.StartsWith("10000-", StringComparison.Ordinal))
            {
                // outside of what DateTime shows: compare the ticks
                Assert.IsTrue(PgTimestamp.TryGetUtcTicks(text, Zagreb, out var outside));
                var hour = TimeSpan.TicksPerHour;
                Assert.AreEqual(expected.StartsWith("0000-", StringComparison.Ordinal) ? -hour / 2 : DateTime.MaxValue.Ticks + 1 + 4 * hour, outside);
                return;
            }

            Assert.AreEqual(expected, Utc(text, Zagreb));
            Assert.AreEqual(expected, Utc(text, TimeZoneInfo.Utc));
        }

        /// <summary>
        /// By default (UTC) a timestamp is its wall time, unchanged.
        /// </summary>
        [TestMethod]
        [DataRow("2026-01-01 00:00:00")]
        [DataRow("2026-03-29 02:30:00")]
        [DataRow("2026-10-25 02:30:00")]
        [DataRow("1950-06-01 12:00:00.25")]
        [DataRow("0001-01-01 00:00:00")]
        [DataRow("9999-12-31 23:59:59.999999")]
        public void TryGetUtcTicks_should_keep_the_wall_time_of_a_timestamp_in_UTC(string text)
        {
            Assert.IsTrue(PgTimestamp.TryParse(text, out var ticks, out _));
            Assert.AreEqual(Format(ticks), Utc(text, TimeZoneInfo.Utc));
        }

        /// <summary>
        /// A timestamp as a time of a time zone, as PostgreSQL's <c>timestamp AT TIME ZONE zone</c> (the expected values
        /// are from PostgreSQL 16): a time skipped when the clock is set forward gets the offset of before, a time that is
        /// there twice the offset of after (the standard offset).
        /// </summary>
        [TestMethod]
        // Zagreb: +1 in winter, +2 in summer; 2026-03-29 02:00 -> 03:00, 2026-10-25 03:00 -> 02:00
        [DataRow("Europe/Zagreb", "2026-01-01 00:00:00", "2025-12-31 23:00:00.0000000")]
        [DataRow("Europe/Zagreb", "2026-01-02 03:04:05.123456", "2026-01-02 02:04:05.1234560")]
        [DataRow("Europe/Zagreb", "2026-06-30 23:59:59", "2026-06-30 21:59:59.0000000")]
        [DataRow("Europe/Zagreb", "2026-03-29 01:59:59", "2026-03-29 00:59:59.0000000")]
        [DataRow("Europe/Zagreb", "2026-03-29 02:00:00", "2026-03-29 01:00:00.0000000")]
        [DataRow("Europe/Zagreb", "2026-03-29 02:30:00", "2026-03-29 01:30:00.0000000")]
        [DataRow("Europe/Zagreb", "2026-03-29 02:59:59.999999", "2026-03-29 01:59:59.9999990")]
        [DataRow("Europe/Zagreb", "2026-03-29 03:00:00", "2026-03-29 01:00:00.0000000")]
        [DataRow("Europe/Zagreb", "2026-10-25 01:59:59", "2026-10-24 23:59:59.0000000")]
        [DataRow("Europe/Zagreb", "2026-10-25 02:00:00", "2026-10-25 01:00:00.0000000")]
        [DataRow("Europe/Zagreb", "2026-10-25 02:30:00", "2026-10-25 01:30:00.0000000")]
        [DataRow("Europe/Zagreb", "2026-10-25 02:59:59", "2026-10-25 01:59:59.0000000")]
        [DataRow("Europe/Zagreb", "2026-10-25 03:00:00", "2026-10-25 02:00:00.0000000")]
        [DataRow("Europe/Zagreb", "1950-06-01 12:00:00.25", "1950-06-01 11:00:00.2500000")]
        // New York: -5 in winter, -4 in summer; 2026-03-08 02:00 -> 03:00, 2026-11-01 02:00 -> 01:00
        [DataRow("America/New_York", "2026-01-01 00:00:00", "2026-01-01 05:00:00.0000000")]
        [DataRow("America/New_York", "2026-03-08 02:30:00", "2026-03-08 07:30:00.0000000")]
        [DataRow("America/New_York", "2026-11-01 01:30:00", "2026-11-01 06:30:00.0000000")]
        [DataRow("America/New_York", "2026-07-04 12:00:00", "2026-07-04 16:00:00.0000000")]
        // Sydney (the southern summer): +11 in summer, +10 in winter; 2026-04-05 03:00 -> 02:00, 2026-10-04 02:00 -> 03:00
        [DataRow("Australia/Sydney", "2026-01-01 00:00:00", "2025-12-31 13:00:00.0000000")]
        [DataRow("Australia/Sydney", "2026-04-05 02:30:00", "2026-04-04 16:30:00.0000000")]
        [DataRow("Australia/Sydney", "2026-10-04 02:30:00", "2026-10-03 16:30:00.0000000")]
        [DataRow("Australia/Sydney", "2026-07-01 00:00:00", "2026-06-30 14:00:00.0000000")]
        // Kolkata: +5:30, no daylight saving time
        [DataRow("Asia/Kolkata", "2026-01-01 00:00:00", "2025-12-31 18:30:00.0000000")]
        [DataRow("Asia/Kolkata", "2026-07-01 00:00:00", "2026-06-30 18:30:00.0000000")]
        public void TryGetUtcTicks_should_take_a_timestamp_as_a_time_of_the_time_zone_as_PostgreSQL(string zone, string text, string expected)
        {
            Assert.AreEqual(expected, Utc(text, TimeZoneInfo.FindSystemTimeZoneById(zone)));
        }

        [TestMethod]
        public void TryGetUtcTicks_should_hold_the_first_hours_of_year_1_in_a_zone_east_of_UTC()
        {
            // before 0001-01-01 00:00 UTC: negative ticks, which a DateTime cannot hold, but a BSON date or a DateTime64 can.
            // Kolkata had its local mean time then, +5:53:28 (PostgreSQL 16: 0001-12-31 18:36:32 BC). TimeZoneInfo has offsets
            // of whole minutes, so .NET takes +5:53: a time before a zone had standard time (the 19th century) can differ
            // from PostgreSQL by less than a minute
            Assert.IsTrue(PgTimestamp.TryGetUtcTicks("0001-01-01 00:30:00", Kolkata, out var utcTicks));
            Assert.AreEqual(-(5 * 3600 + 23 * 60) * TimeSpan.TicksPerSecond, utcTicks);
        }

        [TestMethod]
        public void TryGetUtcTicks_should_not_depend_on_the_time_zone_of_the_machine()
        {
            foreach (var zone in new[] { TimeZoneInfo.Utc, Zagreb, NewYork, Sydney, Kolkata })
            {
                Assert.IsTrue(PgTimestamp.TryGetUtcTicks("2026-01-01 00:00:00+00", zone, out var utcTicks));
                Assert.AreEqual(new DateTime(2026, 1, 1).Ticks, utcTicks, zone.Id);
            }
        }

        /// <summary>
        /// The date at the start of the text: of a date, or of a timestamp (a ClickHouse Date column of a timestamp).
        /// </summary>
        [TestMethod]
        [DataRow("2026-01-01", "2026-01-01")]
        [DataRow("2024-02-29", "2024-02-29")]
        [DataRow("0001-01-01", "0001-01-01")]
        [DataRow("9999-12-31", "9999-12-31")]
        [DataRow("2026-01-02 03:04:05", "2026-01-02")]
        [DataRow("2026-01-02 03:04:05.123456+02", "2026-01-02")]
        public void TryParseDate_should_read_the_date(string text, string expected)
        {
            Assert.IsTrue(PgTimestamp.TryParseDate(text, out var date));
            Assert.AreEqual(expected, date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));
        }

        [TestMethod]
        [DataRow("")]
        [DataRow("infinity")]
        [DataRow("-infinity")]
        [DataRow("10000-01-01")]
        [DataRow("0000-01-01")]
        [DataRow("2026-02-29")]
        [DataRow("2026-13-01")]
        [DataRow("2026-1-1")]
        [DataRow("26-01-01")]
        public void TryParseDate_should_not_read_other_text(string text)
        {
            Assert.IsFalse(PgTimestamp.TryParseDate(text, out _), text);
        }
    }
}
