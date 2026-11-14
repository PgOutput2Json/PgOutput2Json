using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

namespace PgOutput2Json.ClickHouse
{
    /// <summary>
    /// Keeps a copy of the PostgreSQL tables in ClickHouse. Every change is inserted as a new row with a version
    /// (the position in the WAL) and a deleted flag, and ReplacingMergeTree keeps the row with the highest version
    /// per key. A change that is replayed has the same key and version, so it changes nothing: no position is saved.
    /// </summary>
    public class ClickHousePublisher : MessagePublisher
    {
        private const string ToastMarker = "__TOAST__";

        private readonly ClickHousePublisherOptions _options;
        private readonly ClickHouseClient _client;
        private readonly ILogger<ClickHousePublisher>? _logger;

        private readonly Dictionary<string, ClickHouseTable> _tables = [];

        internal ClickHousePublisher(ClickHousePublisherOptions options, ClickHouseClient client, ILogger<ClickHousePublisher>? logger)
        {
            _options = options;
            _client = client;
            _logger = logger;
        }

        public override async Task PublishAsync(JsonMessage msg, CancellationToken token)
        {
            using var doc = JsonDocument.Parse(msg.Json.ToString());

            var root = doc.RootElement;

            var changeType = root.TryGetProperty("c", out var changeTypeElement) ? changeTypeElement.GetString() : null;

            if (changeType == "M") return; // logical decoding message - carries no table and no row

            var tableName = msg.TableName.ToString();

            if (tableName.Length == 0) throw new Exception($"Missing table name for change type '{changeType}'");

            var table = await EnsureTableAsync(tableName, root, token).ConfigureAwait(false);

            // the copied rows have version 0, so any change of the row wins over its copy
            var version = (((UInt128)msg.TxFinalLsn) << 64 | msg.MessageNo).ToString();

            var hasKey = root.TryGetProperty("k", out var keyElement);

            if (changeType == "D")
            {
                if (!hasKey) throw new Exception($"Missing key in a delete of {tableName}");

                AppendDelete(table, keyElement, version);
                return;
            }

            if (changeType != "I" && changeType != "U" && changeType != "C") return;

            if (!root.TryGetProperty("r", out var rowElement)) throw new Exception($"Missing row in a change of {tableName}");

            if (rowElement.GetArrayLength() != table.Columns.Count)
            {
                throw new Exception($"The row of {tableName} has {rowElement.GetArrayLength()} values, the schema has {table.Columns.Count} columns");
            }

            var unchanged = await GetUnchangedValuesAsync(table, rowElement, hasKey ? keyElement : null, token).ConfigureAwait(false);

            // an update of the key comes with the old key: the row with the old key is deleted
            if (hasKey && table.KeyColumns.Count > 0 && !IsSameKey(table, keyElement, rowElement))
            {
                AppendDelete(table, keyElement, version);
            }

            var line = table.Upserts.Append('[');

            for (var i = 0; i < table.Columns.Count; i++)
            {
                if (i > 0) line.Append(',');

                if (unchanged != null && unchanged.TryGetValue(i, out var value))
                {
                    line.Append(value);
                }
                else
                {
                    AppendValue(line, table.Columns[i], rowElement[i]);
                }
            }

            line.Append(",\"").Append(version).Append("\",0]\n");
        }

        public override async Task ConfirmAsync(CancellationToken token)
        {
            foreach (var table in _tables.Values)
            {
                await FlushAsync(table, token).ConfigureAwait(false);
            }
        }

        public override ValueTask DisposeAsync()
        {
            _tables.Clear();

            return ValueTask.CompletedTask;
        }

        private async Task FlushAsync(ClickHouseTable table, CancellationToken token)
        {
            // the deletes go first: when a row is deleted and inserted with the same version, the last insert wins
            if (table.Deletes.Length > 0)
            {
                await _client.ExecuteAsync(table.DeleteQuery, table.Deletes.ToString(), null, token).ConfigureAwait(false);

                table.Deletes.Clear();
            }

            if (table.Upserts.Length > 0)
            {
                await _client.ExecuteAsync(table.UpsertQuery, table.Upserts.ToString(), null, token).ConfigureAwait(false);

                table.Upserts.Clear();
            }
        }

        private async Task<ClickHouseTable> EnsureTableAsync(string tableName, JsonElement root, CancellationToken token)
        {
            if (!root.TryGetProperty("s", out var schemaElement))
            {
                // the schema comes with the first row of a table on every connection
                return _tables.TryGetValue(tableName, out var cached) ? cached : throw new Exception("Missing table schema: " + tableName);
            }

            // the rows that wait were written for the columns the table had before
            if (_tables.TryGetValue(tableName, out var existing))
            {
                await FlushAsync(existing, token).ConfigureAwait(false);
            }

            var table = ClickHouseTable.FromSchema(_options.GetTableName(tableName), schemaElement);

            await _client.ExecuteAsync(table.GetCreateStatement(), null, null, token).ConfigureAwait(false);

            if (table.Columns.Count > table.KeyColumns.Count)
            {
                await _client.ExecuteAsync(table.GetAlterStatement(), null, null, token).ConfigureAwait(false);
            }

            _logger?.LogInformation("Table {Table} is ready for {PgTable}", table.QuotedName, tableName);

            _tables[tableName] = table;

            return table;
        }

        private static void AppendDelete(ClickHouseTable table, JsonElement keyElement, string version)
        {
            if (keyElement.GetArrayLength() != table.KeyColumns.Count)
            {
                throw new Exception($"The key of {table.QuotedName} has {keyElement.GetArrayLength()} values, the schema has {table.KeyColumns.Count} key columns");
            }

            var line = table.Deletes.Append('[');

            for (var i = 0; i < table.KeyColumns.Count; i++)
            {
                AppendValue(line, table.KeyColumns[i], keyElement[i]);

                line.Append(',');
            }

            line.Append('"').Append(version).Append("\",1]\n");
        }

        private static void AppendValue(StringBuilder line, ClickHouseColumn column, JsonElement value)
        {
            if (column.Kind != ColumnKind.Other && value.ValueKind == JsonValueKind.String)
            {
                line.Append('"').Append(Clamp(column.Kind, value.GetString()!)).Append('"');
            }
            else
            {
                line.Append(value.GetRawText());
            }
        }

        /// <summary>
        /// PostgreSQL has infinite dates, dates before Christ and years above 9999. ClickHouse has none of them,
        /// and would reject the row: they become the first or the last day ClickHouse can store.
        /// </summary>
        internal static string Clamp(ColumnKind kind, string value)
        {
            if (value == "-infinity" || value.EndsWith(" BC", StringComparison.Ordinal))
            {
                return kind == ColumnKind.Date ? "0001-01-01" : "0001-01-01 00:00:00";
            }

            if (value == "infinity" || value.IndexOf('-') > 4)
            {
                return kind == ColumnKind.Date ? "9999-12-31" : "9999-12-31 23:59:59.999999";
            }

            return value;
        }

        private static bool IsUnchanged(JsonElement value)
        {
            return value.ValueKind == JsonValueKind.String && value.ValueEquals(ToastMarker);
        }

        private static bool IsSameKey(ClickHouseTable table, JsonElement keyElement, JsonElement rowElement)
        {
            var keyIndex = 0;

            for (var i = 0; i < table.Columns.Count; i++)
            {
                if (!table.Columns[i].IsKey) continue;

                var newValue = rowElement[i];

                // a large value that was not sent did not change
                if (!IsUnchanged(newValue) && newValue.GetRawText() != keyElement[keyIndex].GetRawText()) return false;

                keyIndex++;
            }

            return true;
        }

        /// <summary>
        /// PostgreSQL does not send a large value (TOAST) that an update did not change, and the new row in ClickHouse
        /// needs it. Returns the JSON of those values by the index of the column, or null if the row has them all.
        /// </summary>
        private async Task<Dictionary<int, string>?> GetUnchangedValuesAsync(ClickHouseTable table, JsonElement rowElement, JsonElement? keyElement, CancellationToken token)
        {
            Dictionary<int, string>? values = null;
            List<int>? missing = null;

            var keyIndex = 0;

            for (var i = 0; i < table.Columns.Count; i++)
            {
                var column = table.Columns[i];

                if (IsUnchanged(rowElement[i]))
                {
                    if (column.IsKey && keyElement != null)
                    {
                        // the old key has the value
                        (values ??= [])[i] = keyElement.Value[keyIndex].GetRawText();
                    }
                    else
                    {
                        (missing ??= []).Add(i);
                    }
                }

                if (column.IsKey) keyIndex++;
            }

            if (missing == null) return values;

            values ??= [];

            foreach (var i in missing) values[i] = "null";

            if (table.KeyColumns.Count == 0 || table.HasNullableKey) return values;

            // the row to read may still wait for its insert
            await FlushAsync(table, token).ConfigureAwait(false);

            var parameters = new List<KeyValuePair<string, string>>();
            var conditions = new List<string>();

            keyIndex = 0;

            for (var i = 0; i < table.Columns.Count; i++)
            {
                var column = table.Columns[i];

                if (!column.IsKey) continue;

                // the row is still under its old key, if the update changed the key
                var keyValue = keyElement != null ? keyElement.Value[keyIndex] : rowElement[i];

                var text = keyValue.ValueKind == JsonValueKind.String ? keyValue.GetString()! : keyValue.GetRawText();

                if (column.Kind != ColumnKind.Other) text = Clamp(column.Kind, text);

                conditions.Add($"{column.QuotedName} = {{k{keyIndex}:{column.Type}}}");
                parameters.Add(new($"k{keyIndex}", EscapeParameter(text)));

                keyIndex++;
            }

            var query = $"SELECT {string.Join(", ", missing.Select(i => table.Columns[i].QuotedName))} FROM {table.QuotedName} FINAL " +
                        $"WHERE {string.Join(" AND ", conditions)} FORMAT JSONCompactEachRow";

            var result = await _client.ExecuteAsync(query, null, parameters, token).ConfigureAwait(false);

            if (string.IsNullOrWhiteSpace(result))
            {
                _logger?.LogWarning("An update of {Table} did not change a large value, and ClickHouse does not have the row to take it from. The value is null now.", table.QuotedName);

                return values;
            }

            using var doc = JsonDocument.Parse(result);

            for (var j = 0; j < missing.Count; j++)
            {
                values[missing[j]] = doc.RootElement[j].GetRawText();
            }

            return values;
        }

        /// <summary>
        /// The value of a query parameter is escaped like a value of the TabSeparated format.
        /// </summary>
        private static string EscapeParameter(string value)
        {
            return value.Replace("\\", "\\\\").Replace("\t", "\\t").Replace("\n", "\\n").Replace("\r", "\\r");
        }
    }
}
