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

        [TestMethod]
        public async Task Copied_row_should_replace_an_existing_row_and_table_sync_should_not_move_the_position()
        {
            var path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"pg2j_{System.Guid.NewGuid():N}.db");

            var options = new SqlitePublisherOptions
            {
                ConnectionStringBuilder = new SqliteConnectionStringBuilder($"Data Source={path};Pooling=False")
            };

            try
            {
                await using (var publisher = new SqlitePublisher(options, null))
                {
                    const string schema = "\"s\":[\"test_table\",[\"id\",1,23],[\"name\",0,25]]";

                    // the main stream
                    await publisher.PublishAsync(CreateMessage($"{{\"c\":\"I\",{schema},\"r\":[1,\"one\"]}}", 100, 1), CancellationToken.None);
                    await publisher.ConfirmAsync(CancellationToken.None);

                    // a sync worker: copied rows, and a change that is behind and one that is ahead of the main stream
                    await publisher.PublishAsync(CreateMessage("{\"c\":\"C\",\"r\":[1,\"copied\"]}", 0, 0, isTableSync: true), CancellationToken.None);
                    await publisher.PublishAsync(CreateMessage("{\"c\":\"C\",\"r\":[2,\"two\"]}", 0, 0, isTableSync: true), CancellationToken.None);
                    await publisher.PublishAsync(CreateMessage("{\"c\":\"U\",\"r\":[2,\"dos\"]}", 50, 1, isTableSync: true), CancellationToken.None);
                    await publisher.PublishAsync(CreateMessage("{\"c\":\"I\",\"r\":[3,\"three\"]}", 200, 1, isTableSync: true), CancellationToken.None);
                    await publisher.ConfirmAsync(CancellationToken.None);

                    Assert.AreEqual((100UL, 1UL), await publisher.GetLastPublishedWalSeqAsync(CancellationToken.None));
                }

                await using var connection = new SqliteConnection(options.ConnectionStringBuilder.ConnectionString);
                await connection.OpenAsync();

                await using var command = connection.CreateCommand();
                command.CommandText = "SELECT group_concat(id || '=' || name, ',') FROM (SELECT id, name FROM test_table ORDER BY id)";

                Assert.AreEqual("1=copied,2=dos,3=three", (string?)await command.ExecuteScalarAsync());
            }
            finally
            {
                System.IO.File.Delete(path);
            }
        }

        private static JsonMessage CreateMessage(string json, ulong txFinalLsn = 0, ulong messageNo = 0, bool isTableSync = false)
        {
            var msg = new JsonMessage { TxFinalLsn = txFinalLsn, MessageNo = messageNo, IsTableSync = isTableSync };

            msg.TableName.Append("test_table");
            msg.Json.Append(json);

            return msg;
        }
    }
}