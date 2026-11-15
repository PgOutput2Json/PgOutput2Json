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
            })
            {
                Assert.IsNotNull(ClickHouseBinaryType.Parse(type, TimeZoneInfo.Utc), type);
            }

            // another type, more dimensions, a Nullable array, a time zone this system does not know
            foreach (var type in new[] { "DateTime", "Date", "Enum8('a' = 1)", "Array(Array(Int32))", "Nullable(Array(Int32))", "Map(String, String)", "DateTime64(6, 'Nowhere/Land')" })
            {
                Assert.IsNull(ClickHouseBinaryType.Parse(type, TimeZoneInfo.Utc), type);
            }

            // a DateTime64 without a time zone needs the one of the server
            Assert.IsNull(ClickHouseBinaryType.Parse("DateTime64(6)", null));
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
    }
}
