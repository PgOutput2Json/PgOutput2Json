using System.Collections.Generic;
using System.Text;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace PgOutput2Json.Tests
{
    [TestClass]
    public class TableSyncTest
    {
        private static List<string> GetValues(string line)
        {
            var values = new List<string>();

            TableCopy.GetValues(values, new StringBuilder(), line, null);

            return values;
        }

        [TestMethod]
        public void GetValues_should_keep_empty_values_in_any_position()
        {
            CollectionAssert.AreEqual(new[] { "1", "a" }, GetValues("1\ta"));
            CollectionAssert.AreEqual(new[] { "1", "" }, GetValues("1\t"));
            CollectionAssert.AreEqual(new[] { "", "a" }, GetValues("\ta"));
            CollectionAssert.AreEqual(new[] { "" }, GetValues(""));
        }

        [TestMethod]
        public void GetValues_should_unescape_the_copy_format()
        {
            CollectionAssert.AreEqual(new[] { "a\tb", "c\nd", "e\\f", "\\N" }, GetValues("a\\tb\tc\\nd\te\\\\f\t\\N"));
        }

        [TestMethod]
        public void Sync_slot_prefix_should_use_a_short_main_slot_name_as_it_is()
        {
            Assert.AreEqual("my_slot_sync_", TableSync.GetSyncSlotPrefix("my_slot"));
        }

        [TestMethod]
        public void Sync_slot_prefix_should_leave_room_for_the_oid_and_stay_unique_for_long_names()
        {
            var first = TableSync.GetSyncSlotPrefix(new string('a', 62) + "1");
            var second = TableSync.GetSyncSlotPrefix(new string('a', 62) + "2");

            Assert.AreEqual(63 - 10, first.Length);
            Assert.AreNotEqual(first, second);
            Assert.IsTrue(first.EndsWith("_sync_"));
        }
    }
}
