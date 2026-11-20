using System.Text;
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

        [TestMethod]
        public async Task Position_saved_in_two_values_should_be_read_and_saved_again_as_one()
        {
            var path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"pg2j_{System.Guid.NewGuid():N}.db");

            var options = new SqlitePublisherOptions
            {
                ConnectionStringBuilder = new SqliteConnectionStringBuilder($"Data Source={path};Pooling=False")
            };

            async Task<string?> ConfigAsync(string sql)
            {
                await using var connection = new SqliteConnection(options.ConnectionStringBuilder.ConnectionString);
                await connection.OpenAsync();

                await using var command = connection.CreateCommand();
                command.CommandText = sql;

                return (string?)await command.ExecuteScalarAsync();
            }

            try
            {
                // a store of an earlier version: the two parts of the position under keys of their own
                await ConfigAsync(@"
                    CREATE TABLE __pg2j_config (cfg_key TEXT NOT NULL, cfg_value TEXT, CONSTRAINT __pg2j_config_pk PRIMARY KEY (cfg_key));
                    INSERT INTO __pg2j_config VALUES ('WalEnd', '200'), ('MessageNo', '5');");

                await using (var publisher = new SqlitePublisher(options, null))
                {
                    Assert.AreEqual((200UL, 5UL), await publisher.GetLastPublishedWalSeqAsync(CancellationToken.None));

                    await publisher.PublishAsync(CreateMessage("{\"c\":\"I\",\"s\":[\"test_table\",[\"id\",1,23]],\"r\":[1]}", 300, 1), CancellationToken.None);
                    await publisher.ConfirmAsync(CancellationToken.None);
                }

                Assert.AreEqual("300/1", await ConfigAsync("SELECT cfg_value FROM __pg2j_config WHERE cfg_key = 'Position'"));

                await using (var publisher = new SqlitePublisher(options, null))
                {
                    Assert.AreEqual((300UL, 1UL), await publisher.GetLastPublishedWalSeqAsync(CancellationToken.None));
                }
            }
            finally
            {
                System.IO.File.Delete(path);
            }
        }

        /// <summary>
        /// A file database with pooled connections, as an application has them, and a parent and a child table
        /// with a foreign key, created by the application.
        /// </summary>
        private static async Task WithApplicationDatabaseAsync(System.Func<string, Task> test)
        {
            var path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"pg2j_{System.Guid.NewGuid():N}.db");

            var connectionString = $"Data Source={path}";

            try
            {
                await ExecuteAsync(connectionString, @"
                    CREATE TABLE parent (id INTEGER PRIMARY KEY);
                    CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent (id));");

                await test(connectionString);
            }
            finally
            {
                SqliteConnection.ClearAllPools();
                System.IO.File.Delete(path);
            }
        }

        private static async Task<object?> ExecuteAsync(string connectionString, string sql)
        {
            await using var connection = new SqliteConnection(connectionString);
            await connection.OpenAsync();

            await using var command = connection.CreateCommand();
            command.CommandText = sql;

            return await command.ExecuteScalarAsync();
        }

        private static JsonMessage CreateTableMessage(string table, string json)
        {
            var msg = new JsonMessage { TxFinalLsn = 100, MessageNo = 1 };

            msg.TableName.Append(table);
            msg.Json.Append(json);

            return msg;
        }

        [TestMethod]
        public async Task Foreign_keys_should_not_be_checked_on_the_rows_of_the_replication()
        {
            await WithApplicationDatabaseAsync(async connectionString =>
            {
                var options = new SqlitePublisherOptions { ConnectionStringBuilder = new SqliteConnectionStringBuilder(connectionString) };

                // a child copied before its parent, as the syncs of two tables can do
                await using (var publisher = new SqlitePublisher(options, null))
                {
                    await publisher.PublishAsync(CreateTableMessage("child", "{\"c\":\"C\",\"s\":[\"child\",[\"id\",1,23],[\"parent_id\",0,23]],\"r\":[1,7]}"), CancellationToken.None);
                    await publisher.ConfirmAsync(CancellationToken.None);

                    await publisher.PublishAsync(CreateTableMessage("parent", "{\"c\":\"C\",\"s\":[\"parent\",[\"id\",1,23]],\"r\":[7]}"), CancellationToken.None);
                    await publisher.ConfirmAsync(CancellationToken.None);
                }

                Assert.AreEqual("1:7", await ExecuteAsync(connectionString, "SELECT id || ':' || parent_id FROM child"));

                // the application's own connections still check them
                Assert.AreEqual(1L, await ExecuteAsync(connectionString, "PRAGMA foreign_keys"));
            });
        }

        /// <summary>
        /// Table and column names come from PostgreSQL, where they can have any character, and whoever can create a table
        /// the publication picks up (FOR ALL TABLES, FOR TABLES IN SCHEMA) chooses them: they must not change the SQL.
        /// </summary>
        [TestMethod]
        public async Task Names_with_quotes_should_not_change_the_SQL()
        {
            await WithApplicationDatabaseAsync(async connectionString =>
            {
                await ExecuteAsync(connectionString, "CREATE TABLE victim (id INTEGER); INSERT INTO victim VALUES (1)");

                const string table = "x\"; DROP TABLE victim; --.y'z";
                const string id = "id\"; DROP TABLE victim; --";
                const string name = "na'me\"";
                const string added = "added\" INTEGER; DROP TABLE victim; --";

                var t = J(table);
                var schema = $"\"s\":[{t},[{J(id)},1,23],[{J(name)},0,25]]";
                var schemaAdded = $"\"s\":[{t},[{J(id)},1,23],[{J(name)},0,25],[{J(added)},0,23]]";

                var options = new SqlitePublisherOptions { ConnectionStringBuilder = new SqliteConnectionStringBuilder(connectionString) };

                await using (var publisher = new SqlitePublisher(options, null))
                {
                    var fullName = "public." + table;

                    await publisher.PublishAsync(CreateTableMessage(fullName, $"{{\"c\":\"I\",{schema},\"r\":[1,\"one\"]}}"), CancellationToken.None);
                    await publisher.PublishAsync(CreateTableMessage(fullName, $"{{\"c\":\"I\",\"r\":[2,\"two\"]}}"), CancellationToken.None);
                    await publisher.PublishAsync(CreateTableMessage(fullName, $"{{\"c\":\"U\",\"r\":[1,\"uno\"]}}"), CancellationToken.None);
                    await publisher.PublishAsync(CreateTableMessage(fullName, $"{{\"c\":\"U\",\"k\":[2],\"r\":[3,\"__TOAST__\"]}}"), CancellationToken.None);
                    await publisher.ConfirmAsync(CancellationToken.None);

                    // a new column: the table is altered
                    await publisher.PublishAsync(CreateTableMessage(fullName, $"{{\"c\":\"I\",{schemaAdded},\"r\":[4,\"four\",44]}}"), CancellationToken.None);
                    await publisher.PublishAsync(CreateTableMessage(fullName, $"{{\"c\":\"D\",\"k\":[4]}}"), CancellationToken.None);
                    await publisher.ConfirmAsync(CancellationToken.None);
                }

                Assert.AreEqual(1L, await ExecuteAsync(connectionString, "SELECT count(*) FROM victim"));

                // the table and the columns have the names of PostgreSQL (the table name without the schema)
                Assert.AreEqual("1=uno,3=two", await ExecuteAsync(connectionString,
                    "SELECT group_concat(k || '=' || v, ',') FROM (SELECT \"id\"\"; DROP TABLE victim; --\" AS k, \"na'me\"\"\" AS v FROM \"x\"\"; DROP TABLE victim; --.y'z\" ORDER BY 1)"));

                Assert.AreEqual(1L, await ExecuteAsync(connectionString,
                    "SELECT count(*) FROM pragma_table_info('x\"; DROP TABLE victim; --.y''z') WHERE name = 'added\" INTEGER; DROP TABLE victim; --'"));
            });
        }

        /// <summary>
        /// The attack the quoting stops: a column name that ends the CREATE TABLE and adds a statement. The first change is a
        /// delete, which uses only the key columns, so every statement of the batch is valid and its transaction commits.
        /// </summary>
        [TestMethod]
        public async Task Column_name_should_not_add_a_statement()
        {
            await WithApplicationDatabaseAsync(async connectionString =>
            {
                await ExecuteAsync(connectionString, "CREATE TABLE victim (id INTEGER); INSERT INTO victim VALUES (1)");

                var options = new SqlitePublisherOptions { ConnectionStringBuilder = new SqliteConnectionStringBuilder(connectionString) };

                await using (var publisher = new SqlitePublisher(options, null))
                {
                    var schema = $"\"s\":[\"x\",[\"id\",1,23],[{J("c\" INTEGER); DROP TABLE victim; --")},0,23]]";

                    await publisher.PublishAsync(CreateTableMessage("public.x", $"{{\"c\":\"D\",{schema},\"k\":[1]}}"), CancellationToken.None);
                    await publisher.ConfirmAsync(CancellationToken.None);
                }

                Assert.AreEqual(1L, await ExecuteAsync(connectionString, "SELECT count(*) FROM victim"));
            });
        }

        /// <summary>A JSON string.</summary>
        private static string J(string text)
        {
            var json = new StringBuilder("\"");
            JsonUtils.EscapeText(json, text);
            return json.Append('"').ToString();
        }

        [TestMethod]
        public async Task Writer_should_not_share_the_pooled_connections_of_the_application()
        {
            await WithApplicationDatabaseAsync(async connectionString =>
            {
                // a read-only connection of the application, back in the pool
                await ExecuteAsync(connectionString, "PRAGMA query_only = true");

                var options = new SqlitePublisherOptions { ConnectionStringBuilder = new SqliteConnectionStringBuilder(connectionString) };

                await using (var publisher = new SqlitePublisher(options, null))
                {
                    await publisher.PublishAsync(CreateTableMessage("parent", "{\"c\":\"I\",\"s\":[\"parent\",[\"id\",1,23]],\"r\":[1]}"), CancellationToken.None);
                    await publisher.ConfirmAsync(CancellationToken.None);
                }

                Assert.AreEqual(1L, await ExecuteAsync(connectionString, "SELECT count(*) FROM parent"));

                // and the writer's settings do not end up in the application's connections
                Assert.AreEqual(1L, await ExecuteAsync(connectionString, "PRAGMA foreign_keys"));
            });
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