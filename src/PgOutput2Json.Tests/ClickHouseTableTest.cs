using System;
using System.Linq;

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

        [TestMethod]
        public void Identifiers_should_skip_string_literals()
        {
            CollectionAssert.AreEqual(new[] { "toYYYYMM", "a b", "c", "d" },
                ClickHouseTable.Identifiers("toYYYYMM(`a b`) + \"c\" + 'e''f' + 'x\\'y' + d").ToArray());
        }
    }
}
