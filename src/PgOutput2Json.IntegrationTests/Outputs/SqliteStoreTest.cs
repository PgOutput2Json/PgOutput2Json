using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

using Microsoft.Data.Sqlite;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace PgOutput2Json.IntegrationTests.Outputs
{
    [TestClass]
    public class SqliteStoreTest
    {
        [TestMethod]
        public async Task Sqlite_should_mirror_the_tables()
        {
            var path = Path.Combine(Path.GetTempPath(), $"pg2j_{Guid.NewGuid():N}.db");
            var connectionString = $"Data Source={path};Pooling=False";

            async Task<string> ReadAsync(string table)
            {
                await using var connection = new SqliteConnection(connectionString);
                await connection.OpenAsync();

                await using var command = connection.CreateCommand();

                command.CommandText = $"SELECT * FROM \"{table}\" ORDER BY id";

                var rows = new List<string>();

                try
                {
                    await using var reader = await command.ExecuteReaderAsync();

                    while (await reader.ReadAsync())
                    {
                        var values = new List<string>();

                        for (var i = 0; i < reader.FieldCount; i++)
                        {
                            var value = reader.GetValue(i);

                            values.Add(value switch
                            {
                                DBNull => "NULL",
                                byte[] bytes => "x" + Convert.ToHexString(bytes),
                                string text when text.Length > 100 => $"{text[0]}*{text.Length}",
                                _ => Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture)!,
                            });
                        }

                        rows.Add(string.Join("|", values));
                    }
                }
                catch (SqliteException)
                {
                    return "(no table)";
                }

                return string.Join("\n", rows);
            }

            try
            {
                await StoreScenario.RunAsync("os_sqlite",
                    builder => builder.UseSqlite(options => options.ConnectionStringBuilder = new SqliteConnectionStringBuilder(connectionString)),
                    ReadAsync,
                    expectedAfterCopy: "1|one|1.5|1|[\"a\",\"b c\"]|x0102|x*3000\n2|two|NULL|0|NULL|NULL|NULL",
                    expected: "1|uno|1.5|1|[\"a\",\"b c\"]|x0102|x*3000\n2|NULL|2.25|0|[\"x\"]|NULL|NULL\n30|three|3|NULL|[]|xFF|NULL",
                    expectedKeyLast: "changed|1\nnew|2",
                    expectedCompositeKey: "1|changed|x\n3|new|zz");
            }
            finally
            {
                File.Delete(path);
            }
        }
    }
}
