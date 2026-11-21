using System.Collections.Generic;
using System;
using System.Linq;
using System.Text;
using System.Text.Json;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using PgOutput2Json.ClickHouse;

namespace PgOutput2Json.Tests
{
    [TestClass]
    public class ClickHouseTableTest
    {
        private static ClickHouseColumn Column(string name, bool isKey) => new()
        {
            Name = name,
            QuotedName = ClickHouseTable.Quote(name),
            IsKey = isKey,
            Type = "Int32",
            Kind = ColumnKind.Other,
        };

        // key (tenant, id), and a column v
        private static ClickHouseTable Table(ClickHouseTableOptions? options) =>
            new("t", [Column("id", true), Column("tenant", true), Column("v", false)], options);

        private static ClickHouseTable TableWithoutKey(ClickHouseTableOptions? options) =>
            new("t", [Column("a", false), Column("b", false)], options);

        private static void AssertFails(Func<ClickHouseTable> create, string message)
        {
            var exception = Assert.ThrowsExactly<Exception>(() => create());

            StringAssert.Contains(exception.Message, message);
        }

        [TestMethod]
        public void Create_statement_should_have_the_defaults_without_options()
        {
            Assert.AreEqual(
                "CREATE TABLE IF NOT EXISTS `t` (`id` Int32, `tenant` Int32, `v` Nullable(Int32), _version UInt128, _deleted UInt8, _synced_at DateTime64(6, 'UTC') DEFAULT now64(6)) " +
                "ENGINE = ReplacingMergeTree(_version, _deleted) ORDER BY (`id`, `tenant`)",
                Table(null).GetCreateStatement(null));
        }

        [TestMethod]
        public void Create_statement_should_have_the_order_the_partitions_and_the_engine_of_the_options()
        {
            var table = Table(new() { OrderBy = ["tenant", "id"], PartitionBy = "intDiv(tenant, 100)" });

            StringAssert.EndsWith(table.GetCreateStatement(TimeSpan.FromMinutes(1)),
                "ENGINE = ReplacingMergeTree(_version, _deleted) PARTITION BY intDiv(tenant, 100) ORDER BY (`tenant`, `id`) SETTINGS " +
                "allow_experimental_replacing_merge_with_cleanup = 1, enable_replacing_merge_with_cleanup_for_min_age_to_force_merge = 1, " +
                "min_age_to_force_merge_on_partition_only = 1, min_age_to_force_merge_seconds = 60");

            // the history keeps every row, so there is nothing to clean up
            var history = Table(new() { Engine = ClickHouseTableEngine.MergeTree });

            StringAssert.EndsWith(history.GetCreateStatement(TimeSpan.FromMinutes(1)), "ENGINE = MergeTree ORDER BY (`id`, `tenant`)");
            Assert.IsNull(history.GetCleanupStatement(TimeSpan.FromMinutes(1)));

            var nothing = Table(new() { Engine = ClickHouseTableEngine.Null });

            StringAssert.EndsWith(nothing.GetCreateStatement(TimeSpan.FromMinutes(1)), "ENGINE = Null");
            Assert.IsNull(nothing.GetCleanupStatement(TimeSpan.FromMinutes(1)));
        }

        [TestMethod]
        public void Order_should_have_exactly_the_key_columns()
        {
            const string message = "the ORDER BY must have exactly the key columns (id, tenant)";

            AssertFails(() => Table(new() { OrderBy = ["tenant"] }), message);
            AssertFails(() => Table(new() { OrderBy = ["tenant", "id", "v"] }), message);
            AssertFails(() => Table(new() { OrderBy = ["v", "id"] }), message);
            AssertFails(() => Table(new() { OrderBy = ["id", "id"] }), "the ORDER BY has id twice");
            AssertFails(() => Table(new() { OrderBy = ["id", "x"] }), "the ORDER BY column x is not a column of the table");
        }

        [TestMethod]
        public void Partitions_should_use_only_key_columns()
        {
            AssertFails(() => Table(new() { PartitionBy = "toYYYYMM(v)" }), "the PARTITION BY uses v, which is not a key column");
            AssertFails(() => Table(new() { PartitionBy = "intDiv(`v`, 10)" }), "the PARTITION BY uses v");

            // a name in a string is not a column
            Assert.AreEqual("tenant = 'v'", Table(new() { PartitionBy = "tenant = 'v'" }).PartitionBy);
        }

        [TestMethod]
        public void Null_engine_should_have_no_order_and_no_partitions()
        {
            const string message = "a table with the Null engine has no ORDER BY and no PARTITION BY";

            AssertFails(() => Table(new() { Engine = ClickHouseTableEngine.Null, OrderBy = ["id", "tenant"] }), message);
            AssertFails(() => Table(new() { Engine = ClickHouseTableEngine.Null, PartitionBy = "id" }), message);
        }

        [TestMethod]
        public void Table_without_a_key_should_take_any_order_and_partitions()
        {
            Assert.AreEqual(
                "CREATE TABLE IF NOT EXISTS `t` (`a` Nullable(Int32), `b` Nullable(Int32), _version UInt128, _deleted UInt8, _synced_at DateTime64(6, 'UTC') DEFAULT now64(6)) " +
                "ENGINE = MergeTree ORDER BY tuple()",
                TableWithoutKey(null).GetCreateStatement(null));

            // its columns are Nullable
            StringAssert.EndsWith(TableWithoutKey(new() { OrderBy = ["b"], PartitionBy = "a" }).GetCreateStatement(TimeSpan.FromMinutes(1)),
                "ENGINE = MergeTree PARTITION BY a ORDER BY (`b`) SETTINGS allow_nullable_key = 1");
        }

        /// <summary>
        /// A timestamp column that is a DateTime64 with its own time zone gets its values as the number ClickHouse stores,
        /// converted by the output, also in JSON; one without a time zone, or of another type, gets the text.
        /// </summary>
        [TestMethod]
        [DataRow("Nullable(DateTime64(6, 'Europe/Zagreb'))", false, "\"2026-03-29 02:30:00\"", "1774747800000000")]
        [DataRow("DateTime64(6, 'UTC')", false, "\"2026-03-29 02:30:00\"", "1774751400000000")]
        [DataRow("DateTime64(3, 'Europe/Zagreb')", false, "\"1950-06-01 12:00:00.25\"", "-618065999750")]
        [DataRow("Nullable(DateTime64(6, 'UTC'))", false, "\"1969-12-31 23:59:59.999999+00\"", "-1")]
        [DataRow("Nullable(DateTime64(6, 'UTC'))", false, "\"infinity\"", "253402300799999999")]
        [DataRow("Nullable(DateTime64(6, 'UTC'))", false, "null", "null")]
        [DataRow("Array(Nullable(DateTime64(6, 'Europe/Zagreb')))", true, "[\"2026-10-25 02:30:00\",null]", "[1792891800000000,null]")]
        [DataRow("Nullable(DateTime64(6))", false, "\"2026-03-29 02:30:00\"", "\"2026-03-29 02:30:00\"")]
        [DataRow("Nullable(String)", false, "\"2026-03-29 02:30:00\"", "\"2026-03-29 02:30:00\"")]
        [DataRow("Array(Nullable(DateTime64(6)))", true, "[\"2026-10-25 02:30:00\"]", "[\"2026-10-25 02:30:00\"]")]
        public void Timestamp_with_a_time_zone_in_ClickHouse_should_be_sent_as_the_number_it_stores(string type, bool isArray, string json, string expected)
        {
            var column = new ClickHouseColumn { Name = "a", QuotedName = "`a`", IsKey = false, Type = type, Kind = isArray ? ColumnKind.TimestampArray : ColumnKind.Timestamp };
            var table = new ClickHouseTable("t", [Column("id", true), column], null);

            table.UseExistingTypes(new Dictionary<string, string> { ["id"] = "Int32", ["a"] = type });

            var server = new ClickHouseServerInfo(TimeZoneInfo.Utc, -25567, 120529, -2208988800000000, 253402300799999999);

            using var doc = JsonDocument.Parse(json);

            var line = new StringBuilder();

            ClickHousePublisher.AppendValue(line, column, doc.RootElement, server);

            Assert.AreEqual(expected, line.ToString());
        }

        private static string Append(string type, ColumnKind kind, string json)
        {
            var column = new ClickHouseColumn { Name = "a", QuotedName = "`a`", IsKey = false, Type = type, Kind = kind };

            using var doc = JsonDocument.Parse(json);

            var line = new StringBuilder();

            ClickHousePublisher.AppendValue(line, column, doc.RootElement);

            return line.ToString();
        }

        [TestMethod]
        public void Array_types_should_map_the_arrays_the_library_sends_as_JSON_arrays()
        {
            Assert.AreEqual("DateTime64(6, 'UTC')", ClickHouseTable.GetArrayElementType((uint)PgOid.A_TIMESTAMPOID, -1));
            Assert.AreEqual("DateTime64(6, 'Europe/Zagreb')", ClickHouseTable.GetArrayElementType((uint)PgOid.A_TIMESTAMPOID, -1, "Europe/Zagreb"));
            Assert.AreEqual("DateTime64(6, 'UTC')", ClickHouseTable.GetType((uint)PgOid.TIMESTAMPOID, -1));
            Assert.AreEqual("DateTime64(6, 'Europe/Zagreb')", ClickHouseTable.GetType((uint)PgOid.TIMESTAMPOID, -1, "Europe/Zagreb"));
            Assert.AreEqual("DateTime64(6, 'UTC')", ClickHouseTable.GetType((uint)PgOid.TIMESTAMPTZOID, -1, "Europe/Zagreb"));
            Assert.AreEqual("UTC", ClickHouseTable.GetTimeZoneName(TimeZoneInfo.Utc));
            Assert.AreEqual("Europe/Zagreb", ClickHouseTable.GetTimeZoneName(TimeZoneInfo.FindSystemTimeZoneById("Europe/Zagreb")));
            Assert.AreEqual("Int32", ClickHouseTable.GetArrayElementType((uint)PgOid.A_INT4OID, -1));
            Assert.AreEqual("Decimal(10, 2)", ClickHouseTable.GetArrayElementType((uint)PgOid.A_NUMERICOID, (10 << 16 | 2) + 4));
            Assert.AreEqual("String", ClickHouseTable.GetArrayElementType((uint)PgOid.A_INTERVALOID, -1));
            Assert.AreEqual("String", ClickHouseTable.GetArrayElementType((uint)PgOid.A_BYTEAOID, -1));

            // not an array, or an array the library sends as its PostgreSQL text (enums, user types)
            Assert.IsNull(ClickHouseTable.GetArrayElementType((uint)PgOid.INT4OID, -1));
            Assert.IsNull(ClickHouseTable.GetArrayElementType(16385, -1));
        }

        [TestMethod]
        public void Array_values_should_clamp_their_dates_and_reject_more_dimensions()
        {
            Assert.AreEqual("""["2024-02-29","9999-12-31",null,"0001-01-01"]""",
                Append("Array(Nullable(Date32))", ColumnKind.DateArray, """["2024-02-29","infinity",null,"0044-03-15 BC"]"""));

            Assert.AreEqual("""["0001-01-01 00:00:00"]""", Append("Array(Nullable(DateTime64(6)))", ColumnKind.TimestampArray, """["-infinity"]"""));

            // the text of the other arrays is not touched, and a null array goes to ClickHouse as null (an empty array)
            Assert.AreEqual("""["infinity",null]""", Append("Array(Nullable(String))", ColumnKind.Array, """["infinity",null]"""));
            Assert.AreEqual("null", Append("Array(Nullable(Int32))", ColumnKind.Array, "null"));

            var exception = Assert.ThrowsExactly<Exception>(() => Append("Array(Nullable(Int32))", ColumnKind.Array, "[[1,2],[3,4]]"));

            StringAssert.Contains(exception.Message, "Column a has a multi-dimensional array, and its ClickHouse type Array(Nullable(Int32)) has one dimension");
        }

        [TestMethod]
        public void Array_of_a_type_that_is_not_built_in_should_be_an_array_of_strings()
        {
            // the type name comes after the type modifier: an array is named after its type with an underscore
            using var schema = JsonDocument.Parse("""["public.t",["id",1,23],["moods",0,16384,-1,"_mood"],["mood",0,16385,-1,"mood"],["key",1,16384,-1,"_mood"]]""");

            var table = ClickHouseTable.FromSchema("t", schema.RootElement, useJsonType: true, useArrayTypes: true);

            Assert.AreEqual("Array(Nullable(String)) TextArray, String Other, String Other",
                string.Join(", ", table.Columns.Skip(1).Select(c => $"{c.Type} {c.Kind}")));

            var text = ClickHouseTable.FromSchema("t", schema.RootElement, useJsonType: true, useArrayTypes: false);

            Assert.AreEqual(ColumnKind.Other, text.Columns[1].Kind);

            // the value is the PostgreSQL text of the array
            Assert.AreEqual("""["sad","so so",null,"NULL"]""", Append("Array(Nullable(String))", ColumnKind.TextArray, """ "{sad,\"so so\",NULL,\"NULL\"}" """));
            Assert.AreEqual("[]", Append("Array(Nullable(String))", ColumnKind.TextArray, "\"{}\""));
            Assert.AreEqual("null", Append("Array(Nullable(String))", ColumnKind.TextArray, "null"));

            var exception = Assert.ThrowsExactly<Exception>(() => Append("Array(Nullable(String))", ColumnKind.TextArray, "\"{{sad},{so}}\""));

            StringAssert.Contains(exception.Message, "Column a has a multi-dimensional array");
        }

        [TestMethod]
        public void Array_columns_should_not_be_Nullable()
        {
            var column = new ClickHouseColumn { Name = "a", QuotedName = "`a`", IsKey = false, Type = "Array(Nullable(Int32))", Kind = ColumnKind.Array };

            var table = new ClickHouseTable("t", [Column("id", true), column]);

            StringAssert.Contains(table.GetCreateStatement(null), "`a` Array(Nullable(Int32)),");
            StringAssert.Contains(table.GetAlterStatement(), "ADD COLUMN IF NOT EXISTS `a` Array(Nullable(Int32))");
        }

        [TestMethod]
        public void Identifiers_should_skip_string_literals()
        {
            CollectionAssert.AreEqual(new[] { "toYYYYMM", "a b", "c", "d" },
                ClickHouseTable.Identifiers("toYYYYMM(`a b`) + \"c\" + 'e''f' + 'x\\'y' + d").ToArray());
        }
    }
}
