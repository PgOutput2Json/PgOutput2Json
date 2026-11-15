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

        [TestMethod]
        public void Unchanged_value_and_its_JSON_marker_should_be_the_same()
        {
            using var doc = JsonDocument.Parse("\"__TOAST__\"");

            Assert.AreEqual(SqliteValueKind.Unchanged, SqliteValue.FromJson(doc.RootElement).Kind);
            Assert.AreEqual(SqliteValueKind.Unchanged, SqliteValue.FromRow(RowValue.Unchanged, (uint)PgOid.TEXTOID, new StringBuilder()).Kind);
        }
    }
}
