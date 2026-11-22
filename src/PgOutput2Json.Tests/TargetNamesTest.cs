using System;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace PgOutput2Json.Tests
{
    [TestClass]
    public class TargetNamesTest
    {
        [TestMethod]
        [DataRow("public", "orders", "_", "orders")]
        [DataRow("sales", "orders", "_", "sales_orders")]
        [DataRow("sales", "orders", ".", "sales.orders")]
        [DataRow("x.y", "a.b", "_", "x.y_a.b")]
        [DataRow("Public", "orders", "_", "Public_orders")]
        public void Default_should_keep_the_name_of_a_public_table_and_join_any_other(string schema, string table, string separator, string expected)
        {
            Assert.AreEqual(expected, TargetNames.Default(schema, table, separator));
        }

        [TestMethod]
        public void Get_should_use_the_function_once_per_table()
        {
            var calls = 0;
            var names = new TargetNames((schema, table) => { calls++; return $"{schema}-{table}"; });

            Assert.AreEqual("sales-orders", names.Get("sales.orders", "sales", "orders"));
            Assert.AreEqual("sales-orders", names.Get("sales.orders", "sales", "orders"));
            Assert.AreEqual(1, calls);
        }

        [TestMethod]
        public void Get_should_fail_when_two_tables_have_the_same_name_in_the_output()
        {
            var names = new TargetNames((schema, table) => table);

            Assert.AreEqual("orders", names.Get("public.orders", "public", "orders"));

            var e = Assert.ThrowsExactly<Exception>(() => names.Get("sales.orders", "sales", "orders"));

            StringAssert.Contains(e.Message, "public.orders");
            StringAssert.Contains(e.Message, "sales.orders");
            StringAssert.Contains(e.Message, "GetTableName");
        }

        [TestMethod]
        public void Get_should_fail_when_the_function_returns_no_name()
        {
            var names = new TargetNames((schema, table) => "");

            Assert.ThrowsExactly<Exception>(() => names.Get("public.orders", "public", "orders"));
        }
    }
}
