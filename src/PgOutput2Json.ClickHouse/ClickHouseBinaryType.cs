using System;
using System.Collections.Generic;
using System.Globalization;

namespace PgOutput2Json.ClickHouse
{
    internal enum BinaryBase
    {
        Bool,
        UInt8,
        Int16,
        Int32,
        UInt32,
        Int64,
        UInt128,
        Float32,
        Float64,
        Decimal,
        Uuid,
        Date32,
        DateTime64,
        String,
        Json,
        Date,
        DateTime,
        Enum8,
        Enum16,
        FixedString,
        IPv4,
        IPv6,
    }

    /// <summary>
    /// How a value of a ClickHouse column is written in the RowBinary format. ClickHouse does not convert the values
    /// of RowBinary to the type of the column, so this is made from the type the column really has in ClickHouse.
    /// The types the output creates are covered, and Date, DateTime, Enum8, Enum16, FixedString, IPv4, IPv6 and LowCardinality:
    /// a table with another type (Map, Tuple...) gets JSON.
    /// </summary>
    internal sealed class ClickHouseBinaryType
    {
        public BinaryBase Base { get; private init; }

        public bool IsNullable { get; private init; }

        /// <summary>The type of the elements, for an array.</summary>
        public ClickHouseBinaryType? Element { get; private init; }

        /// <summary>The precision and the scale of a Decimal, the precision of a DateTime64.</summary>
        public int Precision { get; private init; }
        public int Scale { get; private init; }

        /// <summary>The time zone of a DateTime or a DateTime64: the one of the column, or else the one of the server.</summary>
        public TimeZoneInfo? TimeZone { get; private init; }

        /// <summary>The bytes of a FixedString.</summary>
        public int Length { get; private init; }

        /// <summary>The values of an Enum8 or an Enum16, by name.</summary>
        public IReadOnlyDictionary<string, short>? EnumValues { get; private init; }

        /// <summary>
        /// The type, or null if the RowBinary of it is not covered. <paramref name="serverTimeZone"/> is the one
        /// of a DateTime64 without a time zone, and null if it is not known.
        /// </summary>
        public static ClickHouseBinaryType? Parse(string type, TimeZoneInfo? serverTimeZone)
        {
            type = type.Trim();

            // RowBinary has the values of a LowCardinality as those of its type
            if (Unwrap(type, "LowCardinality(", out var inner)) return Parse(inner, serverTimeZone);

            if (Unwrap(type, "Nullable(", out inner))
            {
                var nullable = Parse(inner, serverTimeZone);

                if (nullable == null || nullable.IsNullable || nullable.Element != null) return null;

                return new ClickHouseBinaryType
                {
                    Base = nullable.Base,
                    IsNullable = true,
                    Precision = nullable.Precision,
                    Scale = nullable.Scale,
                    TimeZone = nullable.TimeZone,
                    Length = nullable.Length,
                    EnumValues = nullable.EnumValues,
                };
            }

            if (Unwrap(type, "Array(", out inner))
            {
                var element = Parse(inner, serverTimeZone);

                // one dimension: PostgreSQL does not send the number of dimensions of a column
                return element == null || element.Element != null ? null : new ClickHouseBinaryType { Base = element.Base, Element = element };
            }

            switch (type)
            {
                case "Bool": return new() { Base = BinaryBase.Bool };
                case "UInt8": return new() { Base = BinaryBase.UInt8 };
                case "Int16": return new() { Base = BinaryBase.Int16 };
                case "Int32": return new() { Base = BinaryBase.Int32 };
                case "UInt32": return new() { Base = BinaryBase.UInt32 };
                case "Int64": return new() { Base = BinaryBase.Int64 };
                case "UInt128": return new() { Base = BinaryBase.UInt128 };
                case "Float32": return new() { Base = BinaryBase.Float32 };
                case "Float64": return new() { Base = BinaryBase.Float64 };
                case "UUID": return new() { Base = BinaryBase.Uuid };
                case "Date32": return new() { Base = BinaryBase.Date32 };
                case "String": return new() { Base = BinaryBase.String };
                case "JSON": return new() { Base = BinaryBase.Json };
                case "Date": return new() { Base = BinaryBase.Date };
                case "IPv4": return new() { Base = BinaryBase.IPv4 };
                case "IPv6": return new() { Base = BinaryBase.IPv6 };
                case "DateTime": return serverTimeZone == null ? null : new() { Base = BinaryBase.DateTime, TimeZone = serverTimeZone };
            }

            if (Unwrap(type, "DateTime(", out inner))
            {
                var timeZone = ParseTimeZoneName(inner);

                return timeZone == null ? null : new() { Base = BinaryBase.DateTime, TimeZone = timeZone };
            }

            if (Unwrap(type, "FixedString(", out inner))
            {
                return int.TryParse(inner, NumberStyles.None, CultureInfo.InvariantCulture, out var length) && length > 0
                    ? new() { Base = BinaryBase.FixedString, Length = length }
                    : null;
            }

            if (Unwrap(type, "Enum8(", out inner) || Unwrap(type, "Enum16(", out inner))
            {
                var values = ParseEnumValues(inner);

                return values == null ? null : new() { Base = type.StartsWith("Enum8", StringComparison.Ordinal) ? BinaryBase.Enum8 : BinaryBase.Enum16, EnumValues = values };
            }

            // JSON with parameters, e.g. JSON(max_dynamic_paths = 100)
            if (type.StartsWith("JSON(", StringComparison.Ordinal)) return new() { Base = BinaryBase.Json };

            if (Unwrap(type, "Decimal(", out inner))
            {
                var parts = inner.Split(',');

                if (parts.Length != 2
                    || !int.TryParse(parts[0], NumberStyles.None, CultureInfo.InvariantCulture, out var precision)
                    || !int.TryParse(parts[1].Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out var scale)
                    || precision < 1 || precision > 76 || scale > precision)
                {
                    return null;
                }

                return new() { Base = BinaryBase.Decimal, Precision = precision, Scale = scale };
            }

            if (Unwrap(type, "DateTime64(", out inner))
            {
                var comma = inner.IndexOf(',');

                if (!int.TryParse(comma < 0 ? inner : inner[..comma], NumberStyles.None, CultureInfo.InvariantCulture, out var precision) || precision > 9)
                {
                    return null;
                }

                var timeZone = comma >= 0 ? ParseTimeZoneName(inner[(comma + 1)..]) : serverTimeZone;

                return timeZone == null ? null : new() { Base = BinaryBase.DateTime64, Precision = precision, TimeZone = timeZone };
            }

            return null;
        }

        // 'Europe/Berlin'
        private static TimeZoneInfo? ParseTimeZoneName(string text)
        {
            var name = text.Trim();

            return name.Length >= 2 && name[0] == '\'' && name[^1] == '\'' ? FindTimeZone(name[1..^1]) : null;
        }

        /// <summary>
        /// 'a' = 1, 'b c' = 2, '\'quoted\'' = -3: the names in quotes, with backslash escapes, and their numbers.
        /// </summary>
        internal static Dictionary<string, short>? ParseEnumValues(string text)
        {
            var values = new Dictionary<string, short>(StringComparer.Ordinal);

            var i = 0;

            while (i < text.Length)
            {
                while (i < text.Length && text[i] is ' ' or ',') i++;

                if (i == text.Length) break;

                if (text[i] != '\'') return null;

                var name = new System.Text.StringBuilder();

                for (i++; ; i++)
                {
                    if (i >= text.Length) return null;

                    if (text[i] == '\\' && i + 1 < text.Length)
                    {
                        name.Append(text[++i]);
                    }
                    else if (text[i] == '\'')
                    {
                        i++;
                        break;
                    }
                    else
                    {
                        name.Append(text[i]);
                    }
                }

                while (i < text.Length && text[i] == ' ') i++;

                if (i == text.Length || text[i] != '=') return null;

                var start = ++i;

                while (i < text.Length && text[i] != ',') i++;

                if (!short.TryParse(text.AsSpan(start, i - start).Trim(), NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var number)) return null;

                values[name.ToString()] = number;
            }

            return values.Count > 0 ? values : null;
        }

        /// <summary>
        /// A time zone by its IANA name, as ClickHouse has it, or null if this system does not know it.
        /// </summary>
        public static TimeZoneInfo? FindTimeZone(string name)
        {
            if (name is "UTC" or "Etc/UTC" or "UCT" or "Etc/UCT" or "Zulu" or "Etc/Zulu" or "GMT" or "Etc/GMT") return TimeZoneInfo.Utc;

            try
            {
                return TimeZoneInfo.FindSystemTimeZoneById(name);
            }
            catch (Exception ex) when (ex is TimeZoneNotFoundException or InvalidTimeZoneException)
            {
                return null;
            }
        }

        private static bool Unwrap(string type, string prefix, out string inner)
        {
            if (type.StartsWith(prefix, StringComparison.Ordinal) && type.EndsWith(')'))
            {
                inner = type[prefix.Length..^1].Trim();
                return true;
            }

            inner = "";
            return false;
        }
    }
}
