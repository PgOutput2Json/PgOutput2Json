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

        [TestMethod]
        public void WriteNumber_should_write_infinity_and_nan_as_null()
        {
            var builder = new StringBuilder(256);

            JsonUtils.WriteNumber(builder, "123.45");

            Assert.AreEqual("123.45", builder.ToString());

            builder.Clear();

            JsonUtils.WriteNumber(builder, "Infinity");

            Assert.AreEqual("null", builder.ToString());

            builder.Clear();

            JsonUtils.WriteNumber(builder, "-Infinity");

            Assert.AreEqual("null", builder.ToString());

            builder.Clear();

            JsonUtils.WriteNumber(builder, "NaN");

            Assert.AreEqual("null", builder.ToString());
        }

        [TestMethod]
        public void WriteArrayNumber_should_convert_braces()
        {
            var builder = new StringBuilder(256);

            JsonUtils.WriteArrayOfNumber(builder, "{123.45,Infinity,1,2,3}");

            Assert.AreEqual("[123.45,null,1,2,3]", builder.ToString());
        }

        [TestMethod]
        public void WriteArrayNumber_should_handle_multidimensional_arrays()
        {
            var builder = new StringBuilder(256);

            JsonUtils.WriteArrayOfNumber(builder, "{{123.45,Infinity},{1,2,3}}");

            Assert.AreEqual("[[123.45,null],[1,2,3]]", builder.ToString());
        }

        [TestMethod]
        public void WriteBoolean_should_return_true_or_false()
        {
            var builder = new StringBuilder(256);

            JsonUtils.WriteBoolean(builder, "t");

            Assert.AreEqual("true", builder.ToString());

            builder.Clear();

            JsonUtils.WriteBoolean(builder, "f");

            Assert.AreEqual("false", builder.ToString());
        }

        [TestMethod]
        public void WriteArrayOfBoolean_should_return_true_or_false()
        {
            var builder = new StringBuilder(256);

            JsonUtils.WriteArrayOfBoolean(builder, "{t,f,t}");

            Assert.AreEqual("[true,false,true]", builder.ToString());
        }

        [TestMethod]
        public void WriteArrayOfBoolean_should_handle_multidimensional_arrays()
        {
            var builder = new StringBuilder(256);

            JsonUtils.WriteArrayOfBoolean(builder, "{{t,f,t},{t,f,f}}");

            Assert.AreEqual("[[true,false,true],[true,false,false]]", builder.ToString());
        }

        private static string Write(System.Func<StringBuilder, string, int> writer, string value)
        {
            var builder = new StringBuilder(256);

            writer(builder, value);

            // whatever is written must be valid JSON
            using var json = System.Text.Json.JsonDocument.Parse(builder.ToString());

            return builder.ToString();
        }

        [TestMethod]
        public void Arrays_should_write_a_null_element_as_null()
        {
            Assert.AreEqual("[1,null,3]", Write(JsonUtils.WriteArrayOfNumber, "{1,NULL,3}"));
            Assert.AreEqual("[true,null,false]", Write(JsonUtils.WriteArrayOfBoolean, "{t,NULL,f}"));
            Assert.AreEqual("[[null,1],[2,null]]", Write(JsonUtils.WriteArrayOfNumber, "{{NULL,1},{2,NULL}}"));
        }

        [TestMethod]
        public void WriteArrayText_should_tell_a_null_from_the_text_NULL_and_keep_empty_strings()
        {
            Assert.AreEqual("[\"a\",null,\"NULL\",\"\",\"null\"]", Write((b, v) => JsonUtils.WriteArrayOfText(b, v), "{a,NULL,\"NULL\",\"\",null}"));
            Assert.AreEqual("[\"\"]", Write((b, v) => JsonUtils.WriteArrayOfText(b, v), "{\"\"}"));
            Assert.AreEqual("[]", Write((b, v) => JsonUtils.WriteArrayOfText(b, v), "{}"));
        }

        [TestMethod]
        public void WriteArrayOfByte_should_write_the_hex_digits_of_every_element()
        {
            Assert.AreEqual("[\"01\",null,\"deadbeef\",\"\"]", Write(JsonUtils.WriteArrayOfByte, "{\"\\\\x01\",NULL,\"\\\\xdeadbeef\",\"\\\\x\"}"));
            Assert.AreEqual("[[\"01\"],[\"02\"]]", Write(JsonUtils.WriteArrayOfByte, "{{\"\\\\x01\"},{\"\\\\x02\"}}"));
            Assert.AreEqual("[]", Write(JsonUtils.WriteArrayOfByte, "{}"));
        }
    }
}