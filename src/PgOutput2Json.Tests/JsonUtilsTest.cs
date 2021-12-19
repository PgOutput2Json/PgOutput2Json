using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Text;

namespace PgOutput2Json.Tests
{
    [TestClass]
    public class JsonUtilsTest
    {
        [TestMethod]
        public void WriteText_should_add_quotes()
        {
            var builder = new StringBuilder(256);

            JsonUtils.WriteText(builder, "Hello, World!");

            Assert.AreEqual(@"""Hello, World!""", builder.ToString());
        }

        [TestMethod]
        public void WriteText_should_escape_special_chars()
        {
            var builder = new StringBuilder(256);

            JsonUtils.WriteText(builder, "Hello, \"World!\"\t\\\b\f\n\r");

            Assert.AreEqual(@"""Hello, \""World!\""\t\\\b\f\n\r""", builder.ToString());
        }

        [TestMethod]
        public void WriteArrayText_should_quote_all_values()
        {
            var builder = new StringBuilder(256);

            JsonUtils.WriteArrayOfText(builder, "{One,Two,Three,\"Four,\",Five}");

            Assert.AreEqual("[\"One\",\"Two\",\"Three\",\"Four,\",\"Five\"]", builder.ToString());
        }

        [TestMethod]
        public void WriteArrayText_should_escape_special_chars()
        {
            var builder = new StringBuilder(256);

            JsonUtils.WriteArrayOfText(builder, "{\"O\\\"n\\\\e\",Two,Three,\"Four,\",Five}");

            Assert.AreEqual("[\"O\\\"n\\\\e\",\"Two\",\"Three\",\"Four,\",\"Five\"]", builder.ToString());
        }

        [TestMethod]
        public void WriteArrayText_should_handle_commas_and_curlies()
        {
            var builder = new StringBuilder(256);

            JsonUtils.WriteArrayOfText(builder, "{One,Two,Three,\"Four,\",\"{Five}\"}");

            Assert.AreEqual("[\"One\",\"Two\",\"Three\",\"Four,\",\"{Five}\"]", builder.ToString());
        }

        [TestMethod]
        public void WriteArrayText_should_handle_multidimensional_arrays()
        {
            var builder = new StringBuilder(256);

            JsonUtils.WriteArrayOfText(builder, "{{One,Two},{Three,\"Four,\"},{\"{Five}\",Six}}");

            Assert.AreEqual("[[\"One\",\"Two\"],[\"Three\",\"Four,\"],[\"{Five}\",\"Six\"]]", builder.ToString());
        }
    }
}