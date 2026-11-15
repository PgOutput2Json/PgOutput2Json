using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using PgOutput2Json.ClickHouse;

namespace PgOutput2Json.Tests
{
    [TestClass]
    public class ClickHouseRowBinaryTest
    {
        // the range of ClickHouse 26: the years 1 to 9999
        private static readonly ClickHouseServerInfo Server = new(TimeZoneInfo.Utc, -719162, 2932896, -62135596800000000, 253402300799999999);

        private static ClickHouseColumn Column(ColumnKind kind = ColumnKind.Other) => new() { Name = "c", QuotedName = "`c`", IsKey = false, Type = "", Kind = kind };

        private static byte[] Bytes(RowBinaryWriter writer) => writer.GetSegments().SelectMany(s => s.ToArray()).ToArray();

        /// <summary>
        /// The RowBinary of a JSON value for a ClickHouse type, as hex.
        /// </summary>
        private static string Write(string type, string json, ColumnKind kind = ColumnKind.Other, ClickHouseServerInfo? server = null)
        {
            var binaryType = ClickHouseBinaryType.Parse(type, (server ?? Server).TimeZone) ?? throw new Exception($"{type} is not covered");

            using var doc = JsonDocument.Parse(json);

            var writer = new RowBinaryWriter();

            RowBinaryValues.Write(writer, binaryType, Column(kind), doc.RootElement, server ?? Server);

            return Convert.ToHexString(Bytes(writer)).ToLowerInvariant();
        }

        [TestMethod]
        public void Types_should_be_covered_as_the_output_creates_them_and_others_left_to_JSON()
        {
            foreach (var type in new[]
            {
                "Int32", "Nullable(Int16)", "Nullable(Decimal(76, 38))", "Decimal(12, 4)", "Nullable(DateTime64(6))", "Nullable(DateTime64(6, 'UTC'))",
                "Array(Nullable(Int32))", "Nullable(JSON)", "JSON(max_dynamic_paths = 100)", "LowCardinality(String)", "LowCardinality(Nullable(String))",
                "UInt128", "UInt8", "Nullable(UUID)", "Nullable(Date32)", "DateTime64(3, 'Europe/Berlin')",
                "Date", "DateTime", "Nullable(DateTime('UTC'))", "Enum8('a' = 1, 'b c' = -2)", "Enum16('\\'q\\'' = 300)", "FixedString(4)",
                "LowCardinality(FixedString(2))", "IPv4", "Nullable(IPv6)", "Int8", "UInt16", "UInt64", "Int128", "Int256", "UInt256",
            })
            {
                Assert.IsNotNull(ClickHouseBinaryType.Parse(type, TimeZoneInfo.Utc), type);
            }

            // another type, more dimensions, a Nullable array, a time zone this system does not know
            foreach (var type in new[] { "Tuple(Int32, String)", "Array(Array(Int32))", "Nullable(Array(Int32))", "Map(String, String)", "DateTime64(6, 'Nowhere/Land')", "Enum8()" })
            {
                Assert.IsNull(ClickHouseBinaryType.Parse(type, TimeZoneInfo.Utc), type);
            }

            // a DateTime64 or a DateTime without a time zone needs the one of the server
            Assert.IsNull(ClickHouseBinaryType.Parse("DateTime64(6)", null));
            Assert.IsNull(ClickHouseBinaryType.Parse("DateTime", null));

            CollectionAssert.AreEquivalent(new Dictionary<string, short> { ["a"] = 1, ["b, c"] = -2, ["'q'"] = 300 },
                ClickHouseBinaryType.ParseEnumValues("'a' = 1, 'b, c' = -2, '\\'q\\'' = 300"));
        }

        [TestMethod]
        public void Numbers_should_be_little_endian_and_nulls_flagged()
        {
            Assert.AreEqual("feff", Write("Int16", "-2"));
            Assert.AreEqual("00ffffffffffffff7f", Write("Nullable(Int64)", "9223372036854775807"));
            Assert.AreEqual("01", Write("Nullable(Int64)", "null"));

            // read back from ClickHouse, a 64-bit integer can be a string
            Assert.AreEqual("0100000000000000", Write("Int64", "\"1\""));

            // a null for a column that is not Nullable is the default, as with JSONCompactEachRow
            Assert.AreEqual("00000000", Write("Int32", "null"));

            Assert.AreEqual("000000c03f", Write("Nullable(Float32)", "1.5"));
            Assert.AreEqual("0001", Write("Nullable(Bool)", "true"));

            Assert.AreEqual("ff", Write("Int8", "-1"));
            Assert.AreEqual("ffff", Write("UInt16", "65535"));
            Assert.AreEqual("ffffffffffffffff", Write("UInt64", "18446744073709551615"));
            Assert.AreEqual("ffffffffffffffffffffffffffffffff", Write("Int128", "-1"));
            Assert.AreEqual("0100000000000000" + "0000000000000000", Write("Int128", "\"1\""));
            Assert.AreEqual(new string('f', 64), Write("Int256", "-1"));
            Assert.AreEqual("02" + new string('0', 62), Write("UInt256", "2"));

            // a number the type cannot hold, or with a fraction, is an error, as ClickHouse rejects it
            Assert.ThrowsExactly<OverflowException>(() => Write("UInt64", "-1"));
            Assert.ThrowsExactly<OverflowException>(() => Write("Int8", "128"));
            Assert.ThrowsExactly<FormatException>(() => Write("Int128", "1.5"));
        }

        [TestMethod]
        public void Decimals_should_be_scaled_integers_with_the_digits_beyond_the_scale_cut_off()
        {
            // as ClickHouse wrote them: Decimal(12, 2) is an Int64
            Assert.AreEqual("7b00000000000000", Write("Decimal(12, 2)", "1.239"));
            Assert.AreEqual("85ffffffffffffff", Write("Decimal(12, 2)", "-1.23"));
            Assert.AreEqual("96000000", Write("Decimal(9, 1)", "15"));

            // 128 and 256 bits, two's complement
            Assert.AreEqual("85ffffffffffffffffffffffffffffff", Write("Decimal(20, 2)", "-1.23"));
            Assert.AreEqual("0000000000000000000000000000000000000000000000000000000000000000", Write("Decimal(76, 38)", "0"));
            Assert.AreEqual("ff" + new string('f', 62), Write("Decimal(76, 0)", "-1"));

            Assert.ThrowsExactly<OverflowException>(() => Write("Decimal(4, 2)", "123.4"));
        }

        [TestMethod]
        public void Uuid_should_have_each_half_with_its_lowest_byte_first()
        {
            // as ClickHouse wrote it
            Assert.AreEqual("f84e0b9c99bceea0110a38bdb96b6dbb", Write("UUID", "\"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\""));
        }

        [TestMethod]
        public void Dates_should_be_days_and_times_microseconds_since_1970_within_the_range_of_the_server()
        {
            Assert.AreEqual(19782, RowBinaryValues.GetDate32(Column(), "2024-02-29", Server));
            Assert.AreEqual(Server.Date32Max, RowBinaryValues.GetDate32(Column(), "infinity", Server));
            Assert.AreEqual(Server.Date32Min, RowBinaryValues.GetDate32(Column(), "0044-03-15 BC", Server));
            Assert.AreEqual(Server.Date32Max, RowBinaryValues.GetDate32(Column(), "10000-01-01", Server));

            // an older server cannot store as much: its own first day (1900-01-01)
            var older = Server with { Date32Min = -25567 };
            Assert.AreEqual(-25567, RowBinaryValues.GetDate32(Column(), "1066-10-14", older));

            var utc = ClickHouseBinaryType.Parse("DateTime64(6, 'UTC')", null)!;

            // with an offset, at that offset; read back from ClickHouse, in the time zone of the column
            Assert.AreEqual(1709202896789000, RowBinaryValues.GetDateTime64(Column(), "2024-02-29 12:34:56.789+02", utc, Server));
            Assert.AreEqual(1709202896789000, RowBinaryValues.GetDateTime64(Column(), "2024-02-29 10:34:56.789000", utc, Server));
            Assert.AreEqual(1709202896789000 - 1800_000000, RowBinaryValues.GetDateTime64(Column(), "2024-02-29 12:34:56.789+02:30", utc, Server));
            Assert.AreEqual(Server.DateTime64MaxMicros, RowBinaryValues.GetDateTime64(Column(), "infinity", utc, Server));
            Assert.AreEqual(Server.DateTime64MinMicros, RowBinaryValues.GetDateTime64(Column(), "-infinity", utc, Server));

            // a timestamp without a time zone in the time zone of the server, here an hour ahead in winter
            var berlin = ClickHouseBinaryType.FindTimeZone("Europe/Berlin")!;
            var local = ClickHouseBinaryType.Parse("DateTime64(6)", berlin)!;
            Assert.AreEqual(1709202896789000, RowBinaryValues.GetDateTime64(Column(), "2024-02-29 11:34:56.789", local, Server with { TimeZone = berlin }));

            // another precision, rounded down also before 1970
            var milliseconds = ClickHouseBinaryType.Parse("DateTime64(3, 'UTC')", null)!;
            Assert.AreEqual(1709202896789, RowBinaryValues.GetDateTime64(Column(), "2024-02-29 10:34:56.789999+00", milliseconds, Server));
            Assert.AreEqual(-1, RowBinaryValues.GetDateTime64(Column(), "1969-12-31 23:59:59.999500+00", milliseconds, Server));
        }

        [TestMethod]
        public void Date_DateTime_Enum_FixedString_and_addresses_should_be_as_ClickHouse_writes_them()
        {
            // as ClickHouse wrote them: 10.0.0.1, 10.0.0.2 in an IPv6 column, 2024-02-29 12:34:56+02, 2024-02-29, 'happy', 'ab' in FixedString(4)
            Assert.AreEqual("0100000a", Write("IPv4", "\"10.0.0.1\""));
            Assert.AreEqual("00000000000000000000ffff0a000002", Write("IPv6", "\"10.0.0.2\""));
            Assert.AreEqual("00000000000000000000000000000001", Write("IPv6", "\"::1\""));
            Assert.AreEqual("d05de065", Write("DateTime('UTC')", "\"2024-02-29 12:34:56.789+02\""));
            Assert.AreEqual("464d", Write("Date", "\"2024-02-29\""));
            Assert.AreEqual("02", Write("Enum8('sad' = 1, 'happy' = 2)", "\"happy\""));
            Assert.AreEqual("2c01", Write("Enum16('a' = 300)", "\"a\""));
            Assert.AreEqual("61620000", Write("FixedString(4)", "\"ab\""));

            // read back from ClickHouse, a FixedString has its zero bytes
            Assert.AreEqual("61620000", Write("FixedString(4)", "\"ab\\u0000\\u0000\""));

            // out of the range of a DateTime (1970 to 2106) and a Date (1970 to 2149): its first or its last one, as ClickHouse does with the text
            Assert.AreEqual("00000000", Write("DateTime('UTC')", "\"1900-01-01 00:00:00+00\""));
            Assert.AreEqual("ffffffff", Write("DateTime('UTC')", "\"9999-12-31 23:59:59+00\""));
            Assert.AreEqual("ffffffff", Write("DateTime('UTC')", "\"infinity\""));
            Assert.AreEqual("0000", Write("Date", "\"1900-01-01\""));
            Assert.AreEqual("ffff", Write("Date", "\"9999-12-31\""));

            // what ClickHouse rejects as text too
            StringAssert.Contains(Assert.ThrowsExactly<FormatException>(() => Write("IPv4", "\"10.0.0.0/8\"")).Message, "netmask");
            StringAssert.Contains(Assert.ThrowsExactly<FormatException>(() => Write("IPv4", "\"::1\"")).Message, "not an IPv4");
            StringAssert.Contains(Assert.ThrowsExactly<FormatException>(() => Write("Enum8('a' = 1)", "\"b\"")).Message, "not a value");
            StringAssert.Contains(Assert.ThrowsExactly<FormatException>(() => Write("Enum8('a' = 1)", "null")).Message, "no value 0");
            StringAssert.Contains(Assert.ThrowsExactly<FormatException>(() => Write("FixedString(2)", "\"é\u00e9\"")).Message, "4 bytes");

            // a null for an enum with a 0 is that value
            Assert.AreEqual("00", Write("Enum8('none' = 0, 'a' = 1)", "null"));
        }

        [TestMethod]
        public void Arrays_should_have_their_length_and_one_dimension()
        {
            // as ClickHouse wrote it: 2 elements, 1 and null
            Assert.AreEqual("02000100000000000000" + "01", Write("Array(Nullable(Int64))", "[1,null]"));

            // a null array is an empty one
            Assert.AreEqual("00", Write("Array(Nullable(Int64))", "null"));

            // an array of an enum comes as its PostgreSQL text
            Assert.AreEqual("0200017801", Write("Array(Nullable(String))", "\"{x,NULL}\"", ColumnKind.TextArray));

            var exception = Assert.ThrowsExactly<Exception>(() => Write("Array(Nullable(Int64))", "[[1],[2]]"));
            StringAssert.Contains(exception.Message, "multi-dimensional array");
        }

        [TestMethod]
        public void Strings_and_json_should_be_their_UTF8_with_the_length()
        {
            Assert.AreEqual("0003c3a961", Write("Nullable(String)", "\"éa\""));

            // an array in a String column has its JSON text, as with JSONCompactEachRow
            Assert.AreEqual("055b312c325d", Write("String", "[1,2]"));

            // json: an object on one line, any other value as {"v": value}
            Assert.AreEqual(Convert.ToHexString("\u0007{\"a\":1}"u8).ToLowerInvariant(), Write("JSON", "\"{\\\"a\\\":\\n 1}\""));
            Assert.AreEqual(Convert.ToHexString("\u0007{\"v\":5}"u8).ToLowerInvariant(), Write("JSON", "\"5\""));
        }

        [TestMethod]
        public void Writer_should_put_a_value_that_does_not_fit_in_a_new_segment_and_send_only_what_was_written()
        {
            var writer = new RowBinaryWriter();

            // 65530 bytes, then an Int64 that does not fit in the 6 bytes left
            var text = new string('x', 65527);
            writer.WriteString(text.AsSpan());
            writer.WriteInt64(-1);

            // a string longer than a segment
            var longText = new string('y', 200_000);
            writer.WriteString(longText.AsSpan());

            var bytes = Bytes(writer);

            Assert.AreEqual(writer.Length, bytes.Length);
            Assert.AreEqual(3 + 65527 + 8 + 3 + 200_000, bytes.Length);
            CollectionAssert.AreEqual(Enumerable.Repeat((byte)0xff, 8).ToArray(), bytes[(3 + 65527)..(3 + 65527 + 8)]);
            Assert.IsTrue(bytes[^200_000..].All(b => b == 'y'));

            // a cleared writer starts again from the first segment
            writer.Clear();
            writer.WriteByte(7);

            CollectionAssert.AreEqual(new byte[] { 7 }, Bytes(writer));
        }

        /// <summary>
        /// A value of a row of the library (PostgreSQL's text) gives the RowBinary of the JSON the library writes for it.
        /// </summary>
        [TestMethod]
        [DataRow("Int32", PgOid.INT4OID, "-2")]
        [DataRow("Nullable(Int64)", PgOid.INT8OID, "9223372036854775807")]
        [DataRow("Nullable(Decimal(76, 38))", PgOid.NUMERICOID, "12345678901234567890.123456789")]
        [DataRow("Nullable(Decimal(76, 38))", PgOid.NUMERICOID, "NaN")]
        [DataRow("Nullable(Decimal(12, 4))", PgOid.NUMERICOID, "1.5000")]
        [DataRow("Nullable(Float32)", PgOid.FLOAT4OID, "1.5")]
        [DataRow("Nullable(Float64)", PgOid.FLOAT8OID, "1e+300")]
        [DataRow("Nullable(Float64)", PgOid.FLOAT8OID, "-Infinity")]
        [DataRow("Nullable(Bool)", PgOid.BOOLOID, "t")]
        [DataRow("Nullable(Bool)", PgOid.BOOLOID, "f")]
        [DataRow("Nullable(String)", PgOid.BYTEAOID, "\\xdeadbeef")]
        [DataRow("Nullable(String)", PgOid.TEXTOID, "quote\" back\\ nl\n tab\t unié 😀")]
        [DataRow("Nullable(UUID)", PgOid.UUIDOID, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")]
        [DataRow("Nullable(Date32)", PgOid.DATEOID, "2024-02-29")]
        [DataRow("Nullable(Date32)", PgOid.DATEOID, "infinity")]
        [DataRow("Nullable(DateTime64(6))", PgOid.TIMESTAMPOID, "2024-02-29 12:34:56.789")]
        [DataRow("Nullable(DateTime64(6, 'UTC'))", PgOid.TIMESTAMPTZOID, "2024-02-29 12:34:56.789+02")]
        [DataRow("Nullable(String)", PgOid.TIMEOID, "12:34:56")]
        [DataRow("Nullable(JSON)", PgOid.JSONBOID, "{\"a\": 1}")]
        [DataRow("Nullable(String)", PgOid.JSONBOID, "{\"a\": 1}")]
        [DataRow("Array(Nullable(Int32))", PgOid.A_INT4OID, "{1,NULL,3}")]
        [DataRow("Nullable(String)", PgOid.A_INT4OID, "{1,NULL,3}")]
        [DataRow("Array(Nullable(String))", PgOid.A_TEXTOID, "{a,\"b c\"}")]
        [DataRow("Nullable(String)", (PgOid)50000, "happy")]
        [DataRow("Enum8('happy' = 1, 'sad' = 2)", (PgOid)50000, "happy")]
        [DataRow("Nullable(Int32)", PgOid.INT4OID, null)]
        [DataRow("Array(Nullable(Int32))", PgOid.A_INT4OID, null)]
        public void Text_should_give_the_RowBinary_of_its_JSON(string type, PgOid oid, string? text)
        {
            var kind = type.StartsWith("Array(", StringComparison.Ordinal) ? ColumnKind.Array : type.Contains("JSON") ? ColumnKind.Json : ColumnKind.Other;
            var column = new ClickHouseColumn { Name = "c", QuotedName = "`c`", IsKey = false, Type = "", Kind = kind, TypeId = (uint)oid };

            var binaryType = ClickHouseBinaryType.Parse(type, Server.TimeZone)!;

            var json = new System.Text.StringBuilder();

            if (text == null) json.Append("null"); else JsonUtils.WriteValue(json, oid, text);

            using var doc = JsonDocument.Parse(json.ToString());

            var fromJson = new RowBinaryWriter();
            RowBinaryValues.Write(fromJson, binaryType, column, doc.RootElement, Server);

            var fromText = new RowBinaryWriter();
            RowBinaryValues.Write(fromText, binaryType, column, text == null ? RowValue.Null : RowValue.FromText(text), Server);

            Assert.AreEqual(Convert.ToHexString(Bytes(fromJson)), Convert.ToHexString(Bytes(fromText)), $"{type} {oid} {text}");
        }
    }
}
