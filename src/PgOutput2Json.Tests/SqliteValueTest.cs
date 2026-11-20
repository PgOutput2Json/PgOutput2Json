using System;
using System.Collections;
using System.Text;
using System.Text.Json;

using Microsoft.Data.Sqlite;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using PgOutput2Json.Sqlite;

namespace PgOutput2Json.Tests
{
    [TestClass]
    public class SqliteValueTest
    {
        /// <summary>
        /// A value of a row of the library (PostgreSQL's text) is bound as the JSON the library writes for it.
        /// </summary>
        [TestMethod]
        [DataRow(PgOid.INT4OID, "-2")]
        [DataRow(PgOid.INT8OID, "9223372036854775807")]
        [DataRow(PgOid.NUMERICOID, "12345678901234567890.123456789")]
        [DataRow(PgOid.NUMERICOID, "123456789012345678901234567890.123456789")]
        [DataRow(PgOid.NUMERICOID, "1.50")]
        [DataRow(PgOid.NUMERICOID, "NaN")]
        [DataRow(PgOid.FLOAT8OID, "1e+300")]
        [DataRow(PgOid.FLOAT8OID, "1.5e-05")]
        [DataRow(PgOid.FLOAT4OID, "-Infinity")]
        [DataRow(PgOid.BOOLOID, "t")]
        [DataRow(PgOid.BOOLOID, "f")]
        [DataRow(PgOid.BYTEAOID, "\\xdeadbeef")]
        [DataRow(PgOid.BYTEAOID, "\\x")]
        [DataRow(PgOid.TEXTOID, "quote\" back\\ nl\n tab\t unié 😀")]
        [DataRow(PgOid.TEXTOID, "__TOAST__")]
        [DataRow(PgOid.TIMESTAMPOID, "2024-02-29 12:34:56.789")]
        [DataRow(PgOid.TIMESTAMPTZOID, "2024-02-29 12:34:56.789+02")]
        [DataRow(PgOid.DATEOID, "2024-02-29")]
        [DataRow(PgOid.JSONBOID, "{\"a\": [1, 2]}")]
        [DataRow(PgOid.A_INT4OID, "{1,NULL,3}")]
        [DataRow(PgOid.A_TEXTOID, "{a,\"b c\",NULL}")]
        [DataRow(PgOid.A_BOOLOID, "{t,f}")]
        [DataRow((PgOid)50000, "{happy,sad}")]
        [DataRow(PgOid.INT4OID, null)]
        public void Text_should_be_bound_as_its_JSON(PgOid oid, string? text)
        {
            var column = new ColumnInfo { Name = "c", DataType = (uint)oid, TypeModifier = -1 };

            var json = new StringBuilder();

            if (text == null) json.Append("null"); else JsonUtils.WriteValue(json, oid, text);

            using var doc = JsonDocument.Parse(json.ToString());

            var fromJson = new SqliteParameter();
            SqliteConnectionExtensions.BindValue(fromJson, SqliteValue.FromJson(doc.RootElement), column);

            var fromText = new SqliteParameter();
            SqliteConnectionExtensions.BindValue(fromText, SqliteValue.FromRow(text == null ? RowValue.Null : RowValue.FromText(text), (uint)oid, new StringBuilder()), column);

            Assert.AreEqual(fromJson.Value?.GetType(), fromText.Value?.GetType(), $"{oid} {text}");

            if (fromJson.Value is byte[] bytes)
            {
                CollectionAssert.AreEqual(bytes, (ICollection)fromText.Value!, $"{oid} {text}");
            }
            else
            {
                Assert.AreEqual(fromJson.Value, fromText.Value, $"{oid} {text}");
            }
        }

        /// <summary>
        /// A timestamp without time zone is stored without an offset, whatever the local time zone of the machine,
        /// and reads back as the same wall time, of kind Unspecified. Both paths (JSON and row) give the same text.
        /// </summary>
        [TestMethod]
        [DataRow(PgOid.TIMESTAMPOID, "2026-01-02 03:04:05.123456", "2026-01-02T03:04:05.1234560")]
        [DataRow(PgOid.TIMESTAMPOID, "2026-06-30 23:59:59", "2026-06-30T23:59:59.0000000")]
        [DataRow(PgOid.TIMESTAMPOID, "2026-03-29 02:30:00", "2026-03-29T02:30:00.0000000")] // not a local time in Central Europe
        [DataRow(PgOid.TIMESTAMPOID, "infinity", "infinity")]
        [DataRow(PgOid.TIMESTAMPOID, "0044-03-15 12:00:00 BC", "0044-03-15 12:00:00 BC")]
        [DataRow(PgOid.TIMESTAMPTZOID, "2026-01-02 03:04:05.123456+00", "2026-01-02T03:04:05.1234560+00:00")]
        [DataRow(PgOid.TIMESTAMPTZOID, "2026-06-30 23:59:59+02", "2026-06-30T23:59:59.0000000+02:00")]
        [DataRow(PgOid.TIMESTAMPTZOID, "-infinity", "-infinity")]
        [DataRow(PgOid.DATEOID, "2026-01-02", "2026-01-02")]
        [DataRow(PgOid.TIMEOID, "03:04:05.123456", "03:04:05.123456")]
        [DataRow(PgOid.TIMETZOID, "03:04:05+02", "03:04:05+02")]
        public void Date_and_time_should_be_stored_without_the_local_offset(PgOid oid, string text, string expected)
        {
            var column = new ColumnInfo { Name = "c", DataType = (uint)oid, TypeModifier = -1 };

            var json = new StringBuilder();
            JsonUtils.WriteValue(json, oid, text);

            using var doc = JsonDocument.Parse(json.ToString());

            var fromJson = new SqliteParameter();
            SqliteConnectionExtensions.BindValue(fromJson, SqliteValue.FromJson(doc.RootElement), column);

            var fromText = new SqliteParameter();
            SqliteConnectionExtensions.BindValue(fromText, SqliteValue.FromRow(RowValue.FromText(text), (uint)oid, new StringBuilder()), column);

            Assert.AreEqual(expected, fromJson.Value);
            Assert.AreEqual(expected, fromText.Value);
        }

        [TestMethod]
        public void Timestamp_should_read_back_as_the_same_wall_time()
        {
            var column = new ColumnInfo { Name = "c", DataType = (uint)PgOid.TIMESTAMPOID, TypeModifier = -1 };

            using var connection = new SqliteConnection("Data Source=:memory:");
            connection.Open();

            using var command = connection.CreateCommand();

            command.CommandText = "CREATE TABLE t (c DATETIME); INSERT INTO t VALUES (@p0)";

            var parameter = command.Parameters.AddWithValue("@p0", DBNull.Value);
            SqliteConnectionExtensions.BindValue(parameter, SqliteValue.FromRow(RowValue.FromText("2026-01-02 03:04:05.123456"), (uint)PgOid.TIMESTAMPOID, new StringBuilder()), column);

            command.ExecuteNonQuery();

            command.CommandText = "SELECT c FROM t";

            using var reader = command.ExecuteReader();

            Assert.IsTrue(reader.Read());

            var value = reader.GetDateTime(0);

            Assert.AreEqual(new DateTime(2026, 1, 2, 3, 4, 5).AddTicks(1234560), value);
            Assert.AreEqual(DateTimeKind.Unspecified, value.Kind);
        }

        [TestMethod]
        public void Unchanged_value_and_its_JSON_marker_should_be_the_same()
        {
            using var doc = JsonDocument.Parse("\"__TOAST__\"");

            Assert.AreEqual(SqliteValueKind.Unchanged, SqliteValue.FromJson(doc.RootElement).Kind);
            Assert.AreEqual(SqliteValueKind.Unchanged, SqliteValue.FromRow(RowValue.Unchanged, (uint)PgOid.TEXTOID, new StringBuilder()).Kind);
        }
    }
}
