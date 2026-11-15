using System.Text.Json;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using PgOutput2Json.DynamoDb;

namespace PgOutput2Json.Tests
{
    [TestClass]
    public class DynamoDbAttributeTest
    {
        private static string? Number(string json)
        {
            using var doc = JsonDocument.Parse(json);

            return DynamoDbPublisher.GetAttributeValueFromJsonElement(doc.RootElement, new ColumnInfo()).N;
        }

        [TestMethod]
        public void Number_should_keep_the_digits_of_a_numeric()
        {
            // DynamoDB keeps 38 digits; a double would keep about 15
            Assert.AreEqual("12345678901234567890.123456789", Number("12345678901234567890.123456789"));
            Assert.AreEqual("9223372036854775807", Number("9223372036854775807"));
            Assert.AreEqual("-1.50", Number("-1.50"));
        }
    }
}
