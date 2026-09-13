using System.Threading;
using System.Threading.Tasks;

using Microsoft.Data.Sqlite;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using PgOutput2Json.Sqlite;

namespace PgOutput2Json.Tests
{
    [TestClass]
    public class SqlitePublisherTest
    {
        [TestMethod]
        public async Task PublishAsync_should_reuse_cached_commands_across_transactions()
        {
            var options = new SqlitePublisherOptions
            {
                ConnectionStringBuilder = new SqliteConnectionStringBuilder("Data Source=:memory:")
            };

            await using var publisher = new SqlitePublisher(options, null);

            // first transaction - creates the table and caches the prepared commands
            await publisher.PublishAsync(CreateMessage("{\"c\":\"I\",\"s\":[\"test_table\",[\"id\",1,23],[\"name\",0,25]],\"r\":[1,\"one\"]}"), CancellationToken.None);
            await publisher.ConfirmAsync(CancellationToken.None);

            // subsequent transactions reuse the cached commands - they used to fail
            // with "The transaction object is not associated with the same connection object as this command"

            await publisher.PublishAsync(CreateMessage("{\"c\":\"I\",\"s\":[\"test_table\",[\"id\",1,23],[\"name\",0,25]],\"r\":[2,\"two\"]}"), CancellationToken.None);
            await publisher.ConfirmAsync(CancellationToken.None);

            await publisher.PublishAsync(CreateMessage("{\"c\":\"U\",\"r\":[1,\"uno\"]}"), CancellationToken.None);
            await publisher.ConfirmAsync(CancellationToken.None);

            await publisher.PublishAsync(CreateMessage("{\"c\":\"D\",\"k\":[2]}"), CancellationToken.None);
            await publisher.ConfirmAsync(CancellationToken.None);
        }

        private static JsonMessage CreateMessage(string json)
        {
            var msg = new JsonMessage();

            msg.TableName.Append("test_table");
            msg.Json.Append(json);

            return msg;
        }
    }
}