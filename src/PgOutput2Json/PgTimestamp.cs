using System;

namespace PgOutput2Json
{
    /// <summary>
    /// Reads PostgreSQL's text of a date, a timestamp and a timestamptz, as it is in the replication session (DateStyle ISO):
    /// <c>YYYY-MM-DD</c>, <c>YYYY-MM-DD HH:MM:SS[.ffffff]</c>, with an offset <c>+HH[:MM[:SS]]</c> for a timestamptz.
    /// It never uses the time zone of the machine. Anything else (infinity, a date BC, a year after 9999) is not read.
    /// </summary>
    internal static class PgTimestamp
    {
        /// <summary>
        /// The ticks of the wall time as written, without any time zone, and the offset in seconds (east of UTC
        /// positive) if the text has one. A fraction of a second is cut to microseconds, as PostgreSQL has them.
        /// </summary>
        public static bool TryParse(ReadOnlySpan<char> text, out long ticks, out int? offsetSeconds)
        {
            ticks = 0;
            offsetSeconds = null;

            if (!TryParseDate(ref text, out var date)) return false;

            ticks = date.DayNumber * TimeSpan.TicksPerDay;

            if (text.Length > 0 && text[0] is ' ' or 'T')
            {
                text = text[1..];

                if (!TryParseNumber(ref text, 2, out var hours) || hours > 23 || !Skip(ref text, ':')
                    || !TryParseNumber(ref text, 2, out var minutes) || minutes > 59 || !Skip(ref text, ':')
                    || !TryParseNumber(ref text, 2, out var seconds) || seconds > 59)
                {
                    return false;
                }

                ticks += (hours * 3600L + minutes * 60L + seconds) * TimeSpan.TicksPerSecond;

                if (Skip(ref text, '.'))
                {
                    var digits = 0;
                    var micros = 0L;

                    while (text.Length > 0 && char.IsAsciiDigit(text[0]))
                    {
                        if (digits++ < 6) micros = micros * 10 + (text[0] - '0');
                        text = text[1..];
                    }

                    if (digits == 0) return false;

                    for (; digits < 6; digits++) micros *= 10;

                    ticks += micros * 10;
                }
            }

            if (text.Length == 0) return true;

            if (text is "Z")
            {
                offsetSeconds = 0;
                return true;
            }

            if (text[0] is not ('+' or '-')) return false;

            var sign = text[0] == '-' ? -1 : 1;

            text = text[1..];

            if (!TryParseNumber(ref text, 2, out var offsetHours)) return false;

            var offsetMinutes = 0;
            var offsetSecondsPart = 0;

            if (Skip(ref text, ':') && (!TryParseNumber(ref text, 2, out offsetMinutes) || offsetMinutes > 59)) return false;
            if (Skip(ref text, ':') && (!TryParseNumber(ref text, 2, out offsetSecondsPart) || offsetSecondsPart > 59)) return false;

            if (text.Length > 0) return false;

            offsetSeconds = sign * (offsetHours * 3600 + offsetMinutes * 60 + offsetSecondsPart);
            return true;
        }

        /// <summary>
        /// The UTC ticks of the text: at its offset if it has one (timestamptz), or else as a wall time of
        /// <paramref name="timeZone"/> (timestamp), by PostgreSQL's rules (<see cref="LocalTime"/>).
        /// </summary>
        public static bool TryGetUtcTicks(ReadOnlySpan<char> text, TimeZoneInfo timeZone, out long utcTicks)
        {
            if (!TryParse(text, out var ticks, out var offsetSeconds))
            {
                utcTicks = 0;
                return false;
            }

            utcTicks = offsetSeconds is { } offset ? ticks - offset * TimeSpan.TicksPerSecond : LocalTime.ToUtcTicks(ticks, timeZone);
            return true;
        }

        /// <summary>
        /// The date at the start of the text, <c>YYYY-MM-DD</c>: a real date of the years 1 to 9999. What follows
        /// is not read, so the text of a timestamp gives its date (a ClickHouse Date column of a timestamp).
        /// </summary>
        public static bool TryParseDate(ReadOnlySpan<char> text, out DateOnly date)
        {
            return TryParseDate(ref text, out date);
        }

        private static bool TryParseDate(ref ReadOnlySpan<char> text, out DateOnly date)
        {
            date = default;

            if (!TryParseNumber(ref text, 4, out var year) || !Skip(ref text, '-')
                || !TryParseNumber(ref text, 2, out var month) || !Skip(ref text, '-')
                || !TryParseNumber(ref text, 2, out var day))
            {
                return false;
            }

            if (year < 1 || month < 1 || month > 12 || day < 1 || day > DateTime.DaysInMonth(year, month)) return false;

            date = new DateOnly(year, month, day);
            return true;
        }

        private static bool TryParseNumber(ref ReadOnlySpan<char> text, int digits, out int number)
        {
            number = 0;

            if (text.Length < digits) return false;

            for (var i = 0; i < digits; i++)
            {
                if (!char.IsAsciiDigit(text[i])) return false;

                number = number * 10 + (text[i] - '0');
            }

            text = text[digits..];
            return true;
        }

        private static bool Skip(ref ReadOnlySpan<char> text, char c)
        {
            if (text.Length == 0 || text[0] != c) return false;

            text = text[1..];
            return true;
        }
    }
}
