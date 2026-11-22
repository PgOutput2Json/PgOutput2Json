using System.Text;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace PgOutput2Json.Tests
{
    [TestClass]
    public class PgNameTest
    {
        /// <summary>
        /// Only a name with a dot is quoted (its double quotes doubled), so the joined name is not ambiguous.
        /// </summary>
        [TestMethod]
        [DataRow("public", "orders", "public.orders")]
        [DataRow("Sales", "Order Items", "Sales.Order Items")]
        [DataRow("public", "ab\"c", "public.ab\"c")]
        [DataRow("x.y", "t", "\"x.y\".t")]
        [DataRow("x", "y.t", "x.\"y.t\"")]
        [DataRow("x.y", "a.b", "\"x.y\".\"a.b\"")]
        [DataRow("x", "ab\"c.d", "x.\"ab\"\"c.d\"")]
        [DataRow("\"x.y\"", "t", "\"\"\"x.y\"\"\".t")]
        [DataRow("x", ".", "x.\".\"")]
        public void Append_should_quote_only_a_name_with_a_dot(string schema, string table, string expected)
        {
            var builder = new StringBuilder("prefix:");
            PgName.Append(builder, schema, table);
            Assert.AreEqual("prefix:" + expected, builder.ToString());
        }

        [TestMethod]
        public void Append_should_tell_tables_apart_that_were_the_same_without_quotes()
        {
            static string Joined(string schema, string table)
            {
                var builder = new StringBuilder();
                PgName.Append(builder, schema, table);
                return builder.ToString();
            }

            Assert.AreNotEqual(Joined("a.b", "c"), Joined("a", "b.c"));
        }
    }
}
