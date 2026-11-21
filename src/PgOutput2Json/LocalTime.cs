using System;

namespace PgOutput2Json
{
    /// <summary>
    /// A wall time of a time zone (a timestamp without time zone, read as a time of that zone), as UTC, by PostgreSQL's
    /// rules for <c>AT TIME ZONE</c>: a time that is not there (when the clock is set forward) gets the offset of before
    /// the change, and a time that is there twice (when the clock is set back) the offset of after it.
    /// </summary>
    internal static class LocalTime
    {
        public static long ToUtcTicks(long ticks, TimeZoneInfo timeZone)
        {
            if (timeZone == TimeZoneInfo.Utc) return ticks;

            var local = new DateTime(ticks, DateTimeKind.Unspecified);

            // GetUtcOffset of a time that is there twice is the standard offset, which is the one after the clock was set back
            var offset = timeZone.IsInvalidTime(local) ? timeZone.GetUtcOffset(local.AddHours(-3)) : timeZone.GetUtcOffset(local);

            return ticks - offset.Ticks;
        }
    }
}
