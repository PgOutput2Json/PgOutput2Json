using System;
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
    }

    /// <summary>
    /// How a value of a ClickHouse column is written in the RowBinary format. ClickHouse does not convert the values
    /// of RowBinary to the type of the column, so this is made from the type the column really has in ClickHouse.
    /// Only the types the output creates are covered, and LowCardinality of them: a table with another type gets JSON.
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

        /// <summary>The time zone of a DateTime64: the one of the column, or else the one of the server.</summary>
        public TimeZoneInfo? TimeZone { get; private init; }

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

                var timeZone = serverTimeZone;

                if (comma >= 0)
                {
                    var name = inner[(comma + 1)..].Trim();

                    if (name.Length < 2 || name[0] != '\'' || name[^1] != '\'') return null;

                    timeZone = FindTimeZone(name[1..^1]);
                }

                return timeZone == null ? null : new() { Base = BinaryBase.DateTime64, Precision = precision, TimeZone = timeZone };
            }

            return null;
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
