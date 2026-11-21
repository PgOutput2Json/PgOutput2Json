using System;
using System.Linq;
using System.Globalization;
using System.Numerics;
using System.Text.Json;

namespace PgOutput2Json.ClickHouse
{
    /// <summary>
    /// What the binary values need to know about the server: the time zone of a DateTime64 without one, and the first
    /// and the last date it can store, to which the dates of PostgreSQL that it cannot store are moved (as with text).
    /// </summary>
    internal sealed record ClickHouseServerInfo(TimeZoneInfo? TimeZone, int Date32Min, int Date32Max, long DateTime64MinMicros, long DateTime64MaxMicros);

    /// <summary>
    /// Writes the values of a row, as they come in the JSON of the library or from a read of ClickHouse,
    /// in the RowBinary format of the type the column has in ClickHouse.
    /// </summary>
    /// <summary>
    /// A value for RowBinary: a JSON value, or the text PostgreSQL sends, which then has the kind of the JSON value the library would write for it.
    /// </summary>
    internal readonly struct BinaryInput
    {
        private readonly JsonElement _element;
        private readonly string? _text;
        private readonly bool _isText;

        public JsonValueKind ValueKind { get; }

        public BinaryInput(JsonElement element)
        {
            _element = element;
            ValueKind = element.ValueKind;
        }

        private BinaryInput(string? text, JsonValueKind kind)
        {
            _text = text;
            _isText = true;
            ValueKind = kind;
        }

        public static BinaryInput Null => new(null, JsonValueKind.Null);

        /// <summary>The text of a scalar (not an array, not a bytea).</summary>
        public static BinaryInput FromText(string text, PgOid oid)
        {
            if (oid.IsNumber())
            {
                // JSON has no NaN and no infinity: the library writes them as null
                foreach (var c in text)
                {
                    if (!(c is >= '0' and <= '9' or '+' or '-' or 'e' or 'E' or '.')) return Null;
                }

                return new(text, JsonValueKind.Number);
            }

            if (oid.IsBoolean())
            {
                return text.Length > 0 && text[0] == 't' ? new("true", JsonValueKind.True) : new("false", JsonValueKind.False);
            }

            return new(text, JsonValueKind.String);
        }

        public JsonElement Element => _isText ? throw new InvalidOperationException("A value of text is not an array") : _element;

        public string? GetString() => _isText ? _text : _element.GetString();

        public string GetRawText() => _isText ? _text! : _element.GetRawText();

        public bool TryGetInt64(out long value) => _isText
            ? long.TryParse(_text, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out value)
            : _element.TryGetInt64(out value);

        public bool TryGetInt16(out short value) => _isText
            ? short.TryParse(_text, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out value)
            : _element.TryGetInt16(out value);

        public double GetDouble() => _isText ? double.Parse(_text!, NumberStyles.Float, CultureInfo.InvariantCulture) : _element.GetDouble();
    }

    internal static class RowBinaryValues
    {
        private static readonly long UnixEpochTicks = DateTime.UnixEpoch.Ticks;

        private static readonly int UnixEpochDay = new DateOnly(1970, 1, 1).DayNumber;

        public static void Write(RowBinaryWriter writer, ClickHouseBinaryType type, ClickHouseColumn column, JsonElement value, ClickHouseServerInfo server)
        {
            Write(writer, type, column, new BinaryInput(value), server);
        }

        // the JSON of a value that is read through its JSON
        [ThreadStatic] private static System.Text.StringBuilder? t_json;

        /// <summary>
        /// A value of a row of the library: PostgreSQL's text, read as the code of the JSON reads the value the library writes for it.
        /// An array and a bytea go through their JSON.
        /// </summary>
        public static void Write(RowBinaryWriter writer, ClickHouseBinaryType type, ClickHouseColumn column, RowValue value, ClickHouseServerInfo server)
        {
            var oid = (PgOid)column.TypeId;

            if (value.Kind == RowValueKind.Null)
            {
                Write(writer, type, column, BinaryInput.Null, server);
                return;
            }

            var text = value.Text!;

            if (type.Element != null || oid.IsByte() || ClickHouseTable.IsArray(oid))
            {
                var json = (t_json ??= new System.Text.StringBuilder()).Clear();

                JsonUtils.WriteValue(json, oid, text);

                using var doc = JsonDocument.Parse(json.ToString());

                Write(writer, type, column, new BinaryInput(doc.RootElement), server);
                return;
            }

            Write(writer, type, column, BinaryInput.FromText(text, oid), server);
        }

        private static void Write(RowBinaryWriter writer, ClickHouseBinaryType type, ClickHouseColumn column, BinaryInput value, ClickHouseServerInfo server)
        {
            if (type.Element != null)
            {
                // a null array is an empty one, since ClickHouse arrays are not Nullable
                if (value.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
                {
                    writer.WriteVarUInt(0);
                    return;
                }

                WriteArray(writer, type.Element, column, value.Element, server);
                return;
            }

            WriteNullable(writer, type, column, value, server);
        }

        private static void WriteNullable(RowBinaryWriter writer, ClickHouseBinaryType type, ClickHouseColumn column, BinaryInput value, ClickHouseServerInfo server)
        {
            if (value.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
            {
                if (type.IsNullable)
                {
                    writer.WriteByte(1);
                }
                else
                {
                    // as JSONCompactEachRow does with a null for a column that is not Nullable
                    WriteDefault(writer, type);
                }

                return;
            }

            if (type.IsNullable) writer.WriteByte(0);

            WriteValue(writer, type, column, value, server);
        }

        /// <summary>
        /// One dimension, as in the text formats. A null array is an empty one, since ClickHouse arrays are not Nullable.
        /// </summary>
        private static void WriteArray(RowBinaryWriter writer, ClickHouseBinaryType element, ClickHouseColumn column, JsonElement value, ClickHouseServerInfo server)
        {
            if (value.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
            {
                writer.WriteVarUInt(0);
                return;
            }

            if (value.ValueKind == JsonValueKind.String && column.Kind == ColumnKind.TextArray)
            {
                // an array of a type that is not built in comes as its PostgreSQL text
                var json = new System.Text.StringBuilder();

                JsonUtils.WriteArrayOfText(json, value.GetString());

                using var doc = JsonDocument.Parse(json.ToString());

                WriteArray(writer, element, column, doc.RootElement, server);
                return;
            }

            if (value.ValueKind != JsonValueKind.Array) throw new Exception($"Column {column.Name} has a ClickHouse array type, and the value is not an array: {value.GetRawText()}");

            writer.WriteVarUInt((ulong)value.GetArrayLength());

            foreach (var item in value.EnumerateArray())
            {
                if (item.ValueKind == JsonValueKind.Array)
                {
                    throw new Exception($"Column {column.Name} has a multi-dimensional array, and its ClickHouse type {column.Type} has one dimension. " +
                        "Change the ClickHouse column to String (ALTER TABLE ... MODIFY COLUMN ... String), or set UseArrayTypes to false for new tables.");
                }

                WriteNullable(writer, element, column, new BinaryInput(item), server);
            }
        }

        private static void WriteValue(RowBinaryWriter writer, ClickHouseBinaryType type, ClickHouseColumn column, BinaryInput value, ClickHouseServerInfo server)
        {
            switch (type.Base)
            {
                case BinaryBase.Bool:
                    writer.WriteByte(GetBoolean(column, value) ? (byte)1 : (byte)0);
                    break;

                case BinaryBase.UInt8:
                    writer.WriteByte(checked((byte)GetInt64(column, value)));
                    break;

                case BinaryBase.Int16:
                    writer.WriteInt16(checked((short)GetInt64(column, value)));
                    break;

                case BinaryBase.Int32:
                    writer.WriteInt32(checked((int)GetInt64(column, value)));
                    break;

                case BinaryBase.UInt32:
                    writer.WriteUInt32(checked((uint)GetInt64(column, value)));
                    break;

                case BinaryBase.Int64:
                    writer.WriteInt64(GetInt64(column, value));
                    break;

                case BinaryBase.Int8:
                    writer.WriteByte(unchecked((byte)checked((sbyte)GetInt64(column, value))));
                    break;

                case BinaryBase.UInt16:
                    writer.WriteUInt16(checked((ushort)GetInt64(column, value)));
                    break;

                case BinaryBase.UInt64 or BinaryBase.UInt128 or BinaryBase.Int128 or BinaryBase.Int256 or BinaryBase.UInt256:
                    WriteWideInteger(writer, type, column, GetText(value));
                    break;

                case BinaryBase.Float32:
                    writer.WriteFloat32((float)GetDouble(column, value));
                    break;

                case BinaryBase.Float64:
                    writer.WriteFloat64(GetDouble(column, value));
                    break;

                case BinaryBase.Decimal:
                    WriteDecimal(writer, type, column, GetText(value));
                    break;

                case BinaryBase.Uuid:
                    WriteUuid(writer, column, GetText(value));
                    break;

                case BinaryBase.Date32:
                    writer.WriteInt32(GetDate32(column, GetText(value), server));
                    break;

                case BinaryBase.DateTime64:
                    writer.WriteInt64(GetDateTime64(column, GetText(value), type, server));
                    break;

                case BinaryBase.Date:
                    writer.WriteUInt16(GetDate(column, GetText(value)));
                    break;

                case BinaryBase.DateTime:
                    writer.WriteUInt32(GetDateTime(column, GetText(value), type));
                    break;

                case BinaryBase.Enum8:
                    writer.WriteByte(unchecked((byte)checked((sbyte)GetEnumValue(type, column, value))));
                    break;

                case BinaryBase.Enum16:
                    writer.WriteInt16(GetEnumValue(type, column, value));
                    break;

                case BinaryBase.FixedString:
                    WriteFixedString(writer, type, column, GetText(value));
                    break;

                case BinaryBase.IPv4 or BinaryBase.IPv6:
                    WriteAddress(writer, type, column, GetText(value));
                    break;

                case BinaryBase.String:
                    if (value.ValueKind == JsonValueKind.String)
                    {
                        writer.WriteString(value.GetString().AsSpan());
                    }
                    else
                    {
                        // an array, a number or a boolean in a String column has its JSON text, as with JSONCompactEachRow
                        writer.WriteString(value.GetRawText().AsSpan());
                    }
                    break;

                case BinaryBase.Json:
                    if (value.ValueKind == JsonValueKind.Object)
                    {
                        // read from ClickHouse: as it is stored
                        writer.WriteString(value.GetRawText().AsSpan());
                    }
                    else
                    {
                        // a json column of PostgreSQL: its JSON text, without what ClickHouse rejects, read with input_format_binary_read_json_as_string
                        var json = value.ValueKind == JsonValueKind.String ? value.GetString()! : value.GetRawText();

                        writer.WriteString(ClickHousePublisher.NormalizeJson(json, checkDuplicateKeys: !column.IsJsonb));
                    }
                    break;

                default:
                    throw new NotSupportedException($"RowBinary of {type.Base}");
            }
        }

        private static void WriteDefault(RowBinaryWriter writer, ClickHouseBinaryType type)
        {
            switch (type.Base)
            {
                case BinaryBase.Bool or BinaryBase.UInt8: writer.WriteByte(0); break;
                case BinaryBase.Int16: writer.WriteInt16(0); break;
                case BinaryBase.Int32 or BinaryBase.UInt32 or BinaryBase.Date32: writer.WriteInt32(0); break;
                case BinaryBase.Int64 or BinaryBase.DateTime64: writer.WriteInt64(0); break;
                case BinaryBase.UInt128 or BinaryBase.Int128 or BinaryBase.Uuid: writer.WriteUInt64(0); writer.WriteUInt64(0); break;
                case BinaryBase.Int8: writer.WriteByte(0); break;
                case BinaryBase.UInt16: writer.WriteUInt16(0); break;
                case BinaryBase.UInt64: writer.WriteUInt64(0); break;
                case BinaryBase.Int256 or BinaryBase.UInt256: for (var i = 0; i < 4; i++) writer.WriteUInt64(0); break;
                case BinaryBase.Float32: writer.WriteFloat32(0); break;
                case BinaryBase.Float64: writer.WriteFloat64(0); break;
                case BinaryBase.Decimal: WriteUnscaled(writer, type, BigInteger.Zero); break;
                case BinaryBase.String: writer.WriteVarUInt(0); break;
                case BinaryBase.Json: writer.WriteString("{}".AsSpan()); break;
                case BinaryBase.Date: writer.WriteUInt16(0); break;
                case BinaryBase.DateTime or BinaryBase.IPv4: writer.WriteUInt32(0); break;
                case BinaryBase.IPv6: writer.WriteUInt64(0); writer.WriteUInt64(0); break;
                case BinaryBase.FixedString: for (var i = 0; i < type.Length; i++) writer.WriteByte(0); break;
                // ClickHouse rejects the 0 that a null is, unless the enum has it
                case BinaryBase.Enum8 or BinaryBase.Enum16 when type.EnumValues!.Values.Contains((short)0):
                    if (type.Base == BinaryBase.Enum8) writer.WriteByte(0); else writer.WriteInt16(0);
                    break;
                case BinaryBase.Enum8 or BinaryBase.Enum16:
                    throw new FormatException("A null for an Enum column that is not Nullable, and the enum has no value 0");
                default: throw new NotSupportedException($"RowBinary of {type.Base}");
            }
        }

        /// <summary>
        /// The text of a value: a string as it is, anything else as its JSON (a number as written, e.g. 1.50).
        /// </summary>
        private static string GetText(BinaryInput value) => value.ValueKind == JsonValueKind.String ? value.GetString()! : value.GetRawText();

        private static bool GetBoolean(ClickHouseColumn column, BinaryInput value)
        {
            switch (value.ValueKind)
            {
                case JsonValueKind.True: return true;
                case JsonValueKind.False: return false;
                case JsonValueKind.Number: return value.GetDouble() != 0;
            }

            return GetText(value) switch
            {
                "true" or "t" or "1" => true,
                "false" or "f" or "0" => false,
                var text => throw new FormatException($"Column {column.Name}: {text} is not a boolean"),
            };
        }

        private static long GetInt64(ClickHouseColumn column, BinaryInput value)
        {
            if (value.ValueKind == JsonValueKind.Number && value.TryGetInt64(out var number)) return number;

            var text = GetText(value);

            return long.TryParse(text, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out number)
                ? number
                : throw new FormatException($"Column {column.Name}: {text} is not an integer");
        }

        /// <summary>
        /// UInt64 and the integers of 128 and 256 bits, two's complement with the lowest byte first.
        /// </summary>
        private static void WriteWideInteger(RowBinaryWriter writer, ClickHouseBinaryType type, ClickHouseColumn column, string text)
        {
            if (!BigInteger.TryParse(text, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var number))
            {
                throw new FormatException($"Column {column.Name}: {text} is not an integer");
            }

            var (bytes, signed) = type.Base switch
            {
                BinaryBase.UInt64 => (8, false),
                BinaryBase.UInt128 => (16, false),
                BinaryBase.Int128 => (16, true),
                BinaryBase.Int256 => (32, true),
                _ => (32, false),
            };

            var bits = bytes * 8;

            var min = signed ? -(BigInteger.One << (bits - 1)) : BigInteger.Zero;
            var max = signed ? (BigInteger.One << (bits - 1)) - 1 : (BigInteger.One << bits) - 1;

            if (number < min || number > max) throw new OverflowException($"Column {column.Name}: {text} does not fit {type.Base}");

            Span<byte> buffer = stackalloc byte[32];

            buffer.Fill(number.Sign < 0 ? (byte)0xff : (byte)0);

            number.TryWriteBytes(buffer, out _, isUnsigned: !signed, isBigEndian: false);

            writer.WriteFixed(buffer[..bytes]);
        }

        private static double GetDouble(ClickHouseColumn column, BinaryInput value)
        {
            if (value.ValueKind == JsonValueKind.Number) return value.GetDouble();

            var text = GetText(value);

            return text.ToLowerInvariant() switch
            {
                "nan" => double.NaN,
                "inf" or "+inf" or "infinity" => double.PositiveInfinity,
                "-inf" or "-infinity" => double.NegativeInfinity,
                _ => double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out var number)
                    ? number
                    : throw new FormatException($"Column {column.Name}: {text} is not a number"),
            };
        }

        /// <summary>
        /// A Decimal is its value times 10 to the scale, as an integer of 32, 64, 128 or 256 bits by the precision.
        /// Digits beyond the scale are cut off, as ClickHouse does with the text.
        /// </summary>
        private static void WriteDecimal(RowBinaryWriter writer, ClickHouseBinaryType type, ClickHouseColumn column, string text)
        {
            var span = text.AsSpan().Trim();

            var negative = span.Length > 0 && span[0] == '-';

            if (span.Length > 0 && span[0] is '-' or '+') span = span[1..];

            var point = span.IndexOf('.');

            var integerDigits = point < 0 ? span : span[..point];
            var fractionDigits = point < 0 ? ReadOnlySpan<char>.Empty : span[(point + 1)..];

            integerDigits = integerDigits.TrimStart('0');

            if (integerDigits.Length + fractionDigits.Length == 0 && span.Length == 0
                || !IsDigits(integerDigits) || !IsDigits(fractionDigits))
            {
                throw new FormatException($"Column {column.Name}: {text} is not a decimal for {type.Precision} digits");
            }

            if (integerDigits.Length > type.Precision - type.Scale)
            {
                throw new OverflowException($"Column {column.Name}: {text} has more digits than Decimal({type.Precision}, {type.Scale})");
            }

            // the unscaled value: the integer digits, then the first <scale> digits of the fraction, padded with zeros
            if (type.Precision <= 18)
            {
                var small = 0L;

                foreach (var digit in integerDigits) small = small * 10 + (digit - '0');

                for (var i = 0; i < type.Scale; i++) small = small * 10 + (i < fractionDigits.Length ? fractionDigits[i] - '0' : 0);

                if (negative) small = -small;

                if (type.Precision <= 9) writer.WriteInt32((int)small); else writer.WriteInt64(small);

                return;
            }

            var unscaled = BigInteger.Zero;

            foreach (var digit in integerDigits) unscaled = unscaled * 10 + (digit - '0');

            for (var i = 0; i < type.Scale; i++) unscaled = unscaled * 10 + (i < fractionDigits.Length ? fractionDigits[i] - '0' : 0);

            WriteUnscaled(writer, type, negative ? -unscaled : unscaled);
        }

        private static bool IsDigits(ReadOnlySpan<char> text)
        {
            foreach (var c in text)
            {
                if (c is < '0' or > '9') return false;
            }

            return true;
        }

        private static void WriteUnscaled(RowBinaryWriter writer, ClickHouseBinaryType type, BigInteger unscaled)
        {
            if (type.Precision <= 9)
            {
                writer.WriteInt32((int)unscaled);
            }
            else if (type.Precision <= 18)
            {
                writer.WriteInt64((long)unscaled);
            }
            else
            {
                Span<byte> bytes = stackalloc byte[type.Precision <= 38 ? 16 : 32];

                // two's complement, the lowest byte first, filled up with the sign
                bytes.Fill(unscaled.Sign < 0 ? (byte)0xff : (byte)0);

                unscaled.TryWriteBytes(bytes, out _, isUnsigned: false, isBigEndian: false);

                writer.WriteFixed(bytes);
            }
        }

        /// <summary>
        /// ClickHouse has a UUID as two 64-bit integers, the high one first, each with its lowest byte first.
        /// </summary>
        private static void WriteUuid(RowBinaryWriter writer, ClickHouseColumn column, string text)
        {
            if (!Guid.TryParse(text, out var uuid)) throw new FormatException($"Column {column.Name}: {text} is not a UUID");

            Span<byte> bytes = stackalloc byte[16];

            uuid.TryWriteBytes(bytes, bigEndian: true, out _);

            bytes[..8].Reverse();
            bytes[8..].Reverse();

            writer.WriteFixed(bytes);
        }

        /// <summary>
        /// The days since 1970-01-01. A date ClickHouse cannot store (infinity, before Christ, after its last year)
        /// is its first or its last one.
        /// </summary>
        internal static int GetDate32(ClickHouseColumn column, string text, ClickHouseServerInfo server)
        {
            if (IsMinimum(text)) return server.Date32Min;
            if (IsMaximum(text)) return server.Date32Max;

            var span = text.AsSpan();

            if (!TryParseDate(ref span, out var date)) throw new FormatException($"Column {column.Name}: {text} is not a date");

            return Math.Clamp(date.DayNumber - UnixEpochDay, server.Date32Min, server.Date32Max);
        }

        /// <summary>
        /// The time since 1970-01-01 00:00 UTC, in units of the precision of the column. A time with an offset
        /// (timestamptz) is at that offset, one without (timestamp, or read from ClickHouse) in the time zone of the column.
        /// </summary>
        internal static long GetDateTime64(ClickHouseColumn column, string text, ClickHouseBinaryType type, ClickHouseServerInfo server)
        {
            long micros;

            if (IsMinimum(text))
            {
                micros = server.DateTime64MinMicros;
            }
            else if (IsMaximum(text))
            {
                micros = server.DateTime64MaxMicros;
            }
            else
            {
                micros = Math.Clamp(ParseMicros(column, text, type.TimeZone ?? TimeZoneInfo.Utc), server.DateTime64MinMicros, server.DateTime64MaxMicros);
            }

            if (type.Precision == 6) return micros;

            if (type.Precision < 6)
            {
                var divisor = (long)Math.Pow(10, 6 - type.Precision);

                // rounded down, also before 1970
                return micros >= 0 ? micros / divisor : (micros - divisor + 1) / divisor;
            }

            var factor = (long)Math.Pow(10, type.Precision - 6);

            return Math.Clamp(micros, long.MinValue / factor, long.MaxValue / factor) * factor;
        }

        /// <summary>
        /// The days since 1970-01-01 in a UInt16: ClickHouse stores a Date from 1970 to 2149, and moves the others there.
        /// </summary>
        internal static ushort GetDate(ClickHouseColumn column, string text)
        {
            if (IsMinimum(text)) return 0;
            if (IsMaximum(text)) return ushort.MaxValue;

            var span = text.AsSpan();

            if (!TryParseDate(ref span, out var date)) throw new FormatException($"Column {column.Name}: {text} is not a date");

            return (ushort)Math.Clamp(date.DayNumber - UnixEpochDay, 0, ushort.MaxValue);
        }

        /// <summary>
        /// The seconds since 1970-01-01 00:00 UTC in a UInt32: ClickHouse stores a DateTime from 1970 to 2106, and moves the others there.
        /// A fraction of a second is cut off.
        /// </summary>
        internal static uint GetDateTime(ClickHouseColumn column, string text, ClickHouseBinaryType type)
        {
            if (IsMinimum(text)) return 0;
            if (IsMaximum(text)) return uint.MaxValue;

            var micros = ParseMicros(column, text, type.TimeZone ?? TimeZoneInfo.Utc);

            var seconds = micros >= 0 ? micros / 1_000_000 : (micros - 999_999) / 1_000_000;

            return (uint)Math.Clamp(seconds, 0, uint.MaxValue);
        }

        private static short GetEnumValue(ClickHouseBinaryType type, ClickHouseColumn column, BinaryInput value)
        {
            // a number is the value itself
            if (value.ValueKind == JsonValueKind.Number && value.TryGetInt16(out var number)) return number;

            var name = GetText(value);

            return type.EnumValues!.TryGetValue(name, out number)
                ? number
                : throw new FormatException($"Column {column.Name}: '{name}' is not a value of its Enum");
        }

        /// <summary>
        /// Exactly the bytes of the type: the UTF-8 of the value, filled up with zero bytes.
        /// </summary>
        private static void WriteFixedString(RowBinaryWriter writer, ClickHouseBinaryType type, ClickHouseColumn column, string text)
        {
            var length = System.Text.Encoding.UTF8.GetByteCount(text);

            if (length > type.Length) throw new FormatException($"Column {column.Name}: '{text}' has {length} bytes, too many for FixedString({type.Length})");

            var bytes = new byte[type.Length];

            System.Text.Encoding.UTF8.GetBytes(text, bytes);

            writer.WriteBytes(bytes);
        }

        /// <summary>
        /// An IPv4 is a UInt32 with its lowest byte first, an IPv6 its 16 bytes as in the network. An IPv4 address
        /// in an IPv6 column is ::ffff:a.b.c.d. A netmask (inet 10.0.0.0/8, cidr) cannot be stored, as with the text.
        /// </summary>
        private static void WriteAddress(RowBinaryWriter writer, ClickHouseBinaryType type, ClickHouseColumn column, string text)
        {
            if (text.Contains('/')) throw new FormatException($"Column {column.Name}: {text} has a netmask, which {type.Base} cannot store: change the ClickHouse column to String");

            if (!System.Net.IPAddress.TryParse(text, out var address)) throw new FormatException($"Column {column.Name}: {text} is not an IP address");

            Span<byte> bytes = stackalloc byte[16];

            if (type.Base == BinaryBase.IPv4)
            {
                if (address.AddressFamily != System.Net.Sockets.AddressFamily.InterNetwork) throw new FormatException($"Column {column.Name}: {text} is not an IPv4 address");

                address.TryWriteBytes(bytes, out _);

                writer.WriteUInt32(System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(bytes));
                return;
            }

            if (address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork) address = address.MapToIPv6();

            address.TryWriteBytes(bytes, out _);

            writer.WriteFixed(bytes);
        }

        private static bool IsMinimum(string text) => text == "-infinity" || text.EndsWith(" BC", StringComparison.Ordinal);

        // a year of more than four digits, or infinity
        private static bool IsMaximum(string text) => text == "infinity" || text.IndexOf('-') > 4;

        private static long ParseMicros(ClickHouseColumn column, string text, TimeZoneInfo timeZone)
        {
            var span = text.AsSpan();

            if (!TryParseDate(ref span, out var date)) throw new FormatException($"Column {column.Name}: {text} is not a timestamp");

            var ticks = date.ToDateTime(TimeOnly.MinValue).Ticks;

            if (span.Length > 0 && span[0] is ' ' or 'T')
            {
                span = span[1..];

                if (!TryParseNumber(ref span, 2, out var hours) || !Skip(ref span, ':')
                    || !TryParseNumber(ref span, 2, out var minutes) || !Skip(ref span, ':')
                    || !TryParseNumber(ref span, 2, out var seconds))
                {
                    throw new FormatException($"Column {column.Name}: {text} is not a timestamp");
                }

                ticks += (hours * 3600L + minutes * 60L + seconds) * TimeSpan.TicksPerSecond;

                if (Skip(ref span, '.'))
                {
                    // up to microseconds, as PostgreSQL has them
                    var digits = 0;
                    var fraction = 0L;

                    while (span.Length > 0 && char.IsAsciiDigit(span[0]))
                    {
                        if (digits++ < 7) fraction = fraction * 10 + (span[0] - '0');
                        span = span[1..];
                    }

                    for (; digits < 7; digits++) fraction *= 10;

                    ticks += fraction / 10 * 10;
                }
            }

            long utcTicks;

            if (span.Length > 0 && span[0] is '+' or '-' or 'Z')
            {
                utcTicks = ticks - ParseOffsetSeconds(column, text, span) * TimeSpan.TicksPerSecond;
            }
            else if (span.Length == 0)
            {
                utcTicks = LocalTime.ToUtcTicks(ticks, timeZone);
            }
            else
            {
                throw new FormatException($"Column {column.Name}: {text} is not a timestamp");
            }

            return (utcTicks - UnixEpochTicks) / 10;
        }

        // +HH, +HH:MM, +HH:MM:SS, or Z
        private static long ParseOffsetSeconds(ClickHouseColumn column, string text, ReadOnlySpan<char> span)
        {
            if (span is "Z") return 0;

            var sign = span[0] == '-' ? -1 : 1;

            span = span[1..];

            if (!TryParseNumber(ref span, 2, out var hours)) throw new FormatException($"Column {column.Name}: {text} is not a timestamp");

            var minutes = 0;
            var seconds = 0;

            if (Skip(ref span, ':') && !TryParseNumber(ref span, 2, out minutes)) throw new FormatException($"Column {column.Name}: {text} is not a timestamp");
            if (Skip(ref span, ':') && !TryParseNumber(ref span, 2, out seconds)) throw new FormatException($"Column {column.Name}: {text} is not a timestamp");

            if (span.Length > 0) throw new FormatException($"Column {column.Name}: {text} is not a timestamp");

            return sign * (hours * 3600L + minutes * 60L + seconds);
        }

        // YYYY-MM-DD
        private static bool TryParseDate(ref ReadOnlySpan<char> span, out DateOnly date)
        {
            date = default;

            if (!TryParseNumber(ref span, 4, out var year) || !Skip(ref span, '-')
                || !TryParseNumber(ref span, 2, out var month) || !Skip(ref span, '-')
                || !TryParseNumber(ref span, 2, out var day))
            {
                return false;
            }

            if (year < 1 || month < 1 || month > 12 || day < 1 || day > DateTime.DaysInMonth(year, month)) return false;

            date = new DateOnly(year, month, day);
            return true;
        }

        private static bool TryParseNumber(ref ReadOnlySpan<char> span, int digits, out int number)
        {
            number = 0;

            if (span.Length < digits) return false;

            for (var i = 0; i < digits; i++)
            {
                if (!char.IsAsciiDigit(span[i])) return false;

                number = number * 10 + (span[i] - '0');
            }

            span = span[digits..];
            return true;
        }

        private static bool Skip(ref ReadOnlySpan<char> span, char c)
        {
            if (span.Length == 0 || span[0] != c) return false;

            span = span[1..];
            return true;
        }

        /// <summary>
        /// The version of a row: a UInt128, the lowest 64 bits first.
        /// </summary>
        public static void WriteVersion(RowBinaryWriter writer, ulong transactionFinalLsn, ulong messageNo)
        {
            writer.WriteUInt64(messageNo);
            writer.WriteUInt64(transactionFinalLsn);
        }
    }
}
