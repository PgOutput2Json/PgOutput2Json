using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace PgOutput2Json.Tests
{
    [TestClass]
    public class TableSyncStateTest
    {
        [TestMethod]
        public void CombineRowFilters_should_accept_a_row_any_publication_accepts()
        {
            Assert.IsNull(TableSyncState.CombineRowFilters([null]));
            Assert.IsNull(TableSyncState.CombineRowFilters(["(id > 1)", null]));
            Assert.AreEqual("(id > 1)", TableSyncState.CombineRowFilters(["(id > 1)"]));
            Assert.AreEqual("(id > 1)", TableSyncState.CombineRowFilters(["(id > 1)", "(id > 1)"]));
            Assert.AreEqual("((id > 1)) OR ((id < 0))", TableSyncState.CombineRowFilters(["(id > 1)", "(id < 0)"]));
        }
    }
}
