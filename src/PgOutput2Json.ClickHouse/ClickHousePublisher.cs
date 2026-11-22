using System;
using System.Buffers;
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
    public class ClickHousePublisher : MessagePublisher, IRowPublisher
    {
        private const string ToastMarker = "__TOAST__";

        private readonly ClickHousePublisherOptions _options;
        private readonly ClickHouseClient _client;
        private readonly ILogger<ClickHousePublisher>? _logger;

        private readonly Dictionary<string, ClickHouseTable> _tables = [];

        private readonly Utf8Buffer _utf8 = new();

        private string _tableName = "";

        // the time zone and the date range of the server, read once
        private ClickHouseServerInfo? _server;

        // the IANA name of TimestampTimeZone, in the type of the timestamp columns the output creates
        private readonly string _timestampTimeZone;

        // the ClickHouse name of each PostgreSQL table (GetTableName), shared by the publishers of the factory
        internal TargetNames Names { get; init; }

        // the values of the row being written
        private JsonElement[] _row = new JsonElement[16];

        private int ReadValues(JsonElement rowElement)
        {
            var length = 0;

            foreach (var value in rowElement.EnumerateArray())
            {
                if (length == _row.Length) Array.Resize(ref _row, _row.Length * 2);

                _row[length++] = value;
            }

            return length;
        }

        private static bool HasUnchangedValue(ReadOnlySpan<JsonElement> row)
        {
            foreach (var value in row)
            {
                if (IsUnchanged(value)) return true;
            }

            return false;
        }

        internal ClickHousePublisher(ClickHousePublisherOptions options, ClickHouseClient client, ILogger<ClickHousePublisher>? logger)
        {
            _options = options;
            _client = client;
            _logger = logger;
            _timestampTimeZone = ClickHouseTable.GetTimeZoneName(options.TimestampTimeZone);
            Names = new TargetNames(options.GetTableName);
        }

        public override async Task PublishAsync(JsonMessage msg, CancellationToken token)
        {
            using var doc = JsonDocument.Parse(_utf8.Encode(msg.Json));

            var root = doc.RootElement;

            var changeType = root.TryGetProperty("c", out var changeTypeElement) ? changeTypeElement.GetString() : null;

            if (changeType == "M") return; // logical decoding message - carries no table and no row

            _tableName = msg.TableName;

            var tableName = _tableName;

            if (tableName.Length == 0) throw new Exception($"Missing table name for change type '{changeType}'");

            var table = await EnsureTableAsync(tableName, msg.SchemaName, msg.RelationName, root, token).ConfigureAwait(false);

            if (changeType != "I" && changeType != "U" && changeType != "C" && changeType != "D") return;

            JsonElement? rowElement = root.TryGetProperty("r", out var r) ? r : null;
            JsonElement? keyElement = root.TryGetProperty("k", out var k) ? k : null;

            await PublishAsync(table, changeType == "D", rowElement, keyElement, msg.TxFinalLsn, msg.MessageNo, token).ConfigureAwait(false);
        }

        private async Task PublishAsync(ClickHouseTable table, bool isDelete, JsonElement? rowElement, JsonElement? keyElement, ulong txFinalLsn, ulong messageNo, CancellationToken token)
        {
            // the copied rows have version 0, so any change of the row wins over its copy
            var version = GetVersion(txFinalLsn, messageNo);

            if (isDelete)
            {
                if (keyElement == null) throw new Exception($"Missing key in a delete of {table.QuotedName}");

                AppendDelete(table, keyElement.Value, version, txFinalLsn, messageNo);
                return;
            }

            if (rowElement == null) throw new Exception($"Missing row in a change of {table.QuotedName}");

            // read once: indexing a JSON array walks it from the start when it has arrays or objects in it
            var rowLength = ReadValues(rowElement.Value);

            if (rowLength != table.Columns.Count)
            {
                throw new Exception($"The row of {table.QuotedName} has {rowLength} values, the schema has {table.Columns.Count} columns");
            }

            var hasUnchangedValue = HasUnchangedValue(_row.AsSpan(0, rowLength));

            var unchanged = hasUnchangedValue
                ? await GetUnchangedValuesAsync(table, _row, keyElement, token).ConfigureAwait(false)
                : null;

            // an update of the key comes with the old key: the row with the old key is deleted
            if (keyElement != null && table.KeyColumns.Count > 0 && !IsSameKey(table, keyElement.Value, _row))
            {
                AppendDelete(table, keyElement.Value, version, txFinalLsn, messageNo);
            }

            if (table.BinaryTypes != null)
            {
                WriteBinaryRow(table, unchanged, txFinalLsn, messageNo);
                return;
            }

            table.UpsertCount++;

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
                    AppendValue(line, table.Columns[i], _row[i], _server);
                }
            }

            line.Append(",\"").Append(version).Append("\",0]\n");
        }

        private static string GetVersion(ulong txFinalLsn, ulong messageNo) => (((UInt128)txFinalLsn) << 64 | messageNo).ToString();

        // a value of a row as JSON, for the rows that go through the JSON code; and the JSON of a row or a key
        private readonly StringBuilder _json = new();
        private readonly StringBuilder _rowJson = new();
        private readonly StringBuilder _keyJson = new();

        /// <summary>
        /// A row of the library, without its JSON: the values are PostgreSQL's text. A row with an unchanged large value
        /// or with a changed key goes through the code of the JSON rows.
        /// </summary>
        public async Task PublishRowAsync(ChangeRow row, CancellationToken token)
        {
            var table = await EnsureTableAsync(row.Table, token).ConfigureAwait(false);

            var isDelete = row.ChangeType == RowChangeType.Delete;

            var values = isDelete ? null : row.NewValues!;

            if (values != null && values.Count != table.Columns.Count)
            {
                throw new Exception($"The row of {table.QuotedName} has {values.Count} values, the schema has {table.Columns.Count} columns");
            }

            var version = GetVersion(row.TxFinalLsn, row.MessageNo);

            if (isDelete)
            {
                AppendDelete(table, row.OldValues ?? throw new Exception($"Missing key in a delete of {table.QuotedName}"), version, row.TxFinalLsn, row.MessageNo);
                return;
            }

            if (HasUnchangedValue(values!) || (row.OldValues != null && !IsSameKey(table, row)))
            {
                await PublishAsJsonAsync(table, isDelete, row, token).ConfigureAwait(false);
                return;
            }

            table.UpsertCount++;

            if (table.BinaryTypes != null)
            {
                var writer = table.BinaryUpserts!;

                for (var i = 0; i < table.Columns.Count; i++)
                {
                    RowBinaryValues.Write(writer, table.BinaryTypes[i], table.Columns[i], values![i], _server!);
                }

                RowBinaryValues.WriteVersion(writer, row.TxFinalLsn, row.MessageNo);
                writer.WriteByte(0);
                return;
            }

            var line = table.Upserts.Append('[');

            for (var i = 0; i < table.Columns.Count; i++)
            {
                if (i > 0) line.Append(',');

                AppendValue(line, table.Columns[i], values![i]);
            }

            line.Append(",\"").Append(version).Append("\",0]\n");
        }

        /// <summary>
        /// A delete of a row of the library: the old values have the key columns.
        /// </summary>
        private void AppendDelete(ClickHouseTable table, IReadOnlyList<RowValue> oldValues, string version, ulong txFinalLsn, ulong messageNo)
        {
            if (oldValues.Count != table.Columns.Count)
            {
                throw new Exception($"The old row of {table.QuotedName} has {oldValues.Count} values, the schema has {table.Columns.Count} columns");
            }

            table.DeleteCount++;

            if (table.BinaryKeyTypes != null)
            {
                var writer = table.BinaryDeletes!;
                var keyIndex = 0;

                for (var i = 0; i < table.Columns.Count; i++)
                {
                    if (!table.Columns[i].IsKey) continue;

                    RowBinaryValues.Write(writer, table.BinaryKeyTypes[keyIndex++], table.Columns[i], oldValues[i], _server!);
                }

                RowBinaryValues.WriteVersion(writer, txFinalLsn, messageNo);
                writer.WriteByte(1);
                return;
            }

            var line = table.Deletes.Append('[');

            for (var i = 0; i < table.Columns.Count; i++)
            {
                if (!table.Columns[i].IsKey) continue;

                AppendValue(line, table.Columns[i], oldValues[i]);

                line.Append(',');
            }

            line.Append('"').Append(version).Append("\",1]\n");
        }

        private async Task PublishAsJsonAsync(ClickHouseTable table, bool isDelete, ChangeRow row, CancellationToken token)
        {
            JsonDocument? rowDoc = null;
            JsonDocument? keyDoc = null;

            try
            {
                if (row.NewValues != null)
                {
                    WriteJson(_rowJson, table.Columns, row.NewValues, keyOnly: false);
                    rowDoc = JsonDocument.Parse(_rowJson.ToString());
                }

                if (row.OldValues != null)
                {
                    WriteJson(_keyJson, table.Columns, row.OldValues, keyOnly: true);
                    keyDoc = JsonDocument.Parse(_keyJson.ToString());
                }

                await PublishAsync(table, isDelete, rowDoc?.RootElement, keyDoc?.RootElement, row.TxFinalLsn, row.MessageNo, token).ConfigureAwait(false);
            }
            finally
            {
                rowDoc?.Dispose();
                keyDoc?.Dispose();
            }
        }

        /// <summary>
        /// The values as the library writes the row ("r") or the key ("k", the key columns) of the compact JSON.
        /// </summary>
        private static void WriteJson(StringBuilder json, IReadOnlyList<ClickHouseColumn> columns, IReadOnlyList<RowValue> values, bool keyOnly)
        {
            json.Clear().Append('[');

            var first = true;

            for (var i = 0; i < columns.Count; i++)
            {
                if (keyOnly && !columns[i].IsKey) continue;

                if (!first) json.Append(',');
                first = false;

                WriteJsonValue(json, columns[i], values[i]);
            }

            json.Append(']');
        }

        private static void WriteJsonValue(StringBuilder json, ClickHouseColumn column, RowValue value)
        {
            switch (value.Kind)
            {
                case RowValueKind.Null: json.Append("null"); break;
                case RowValueKind.Unchanged: json.Append('"').Append(ToastMarker).Append('"'); break;
                default: JsonUtils.WriteValue(json, (PgOid)column.TypeId, value.Text!); break;
            }
        }

        private static bool HasUnchangedValue(IReadOnlyList<RowValue> values)
        {
            for (var i = 0; i < values.Count; i++)
            {
                if (values[i].Kind == RowValueKind.Unchanged) return true;
            }

            return false;
        }

        private static bool IsSameKey(ClickHouseTable table, ChangeRow row)
        {
            if (table.KeyColumns.Count == 0) return true;

            for (var i = 0; i < table.Columns.Count; i++)
            {
                if (!table.Columns[i].IsKey) continue;

                var newValue = row.NewValues![i];
                var oldValue = row.OldValues![i];

                // a large value that was not sent did not change
                if (newValue.Kind != RowValueKind.Unchanged && (newValue.Kind != oldValue.Kind || newValue.Text != oldValue.Text)) return false;
            }

            return true;
        }

        /// <summary>
        /// A value of a row of the library, from its text, as <see cref="AppendValue(StringBuilder, ClickHouseColumn, JsonElement, ClickHouseServerInfo?)"/>
        /// writes the same value from the JSON.
        /// </summary>
        private void AppendValue(StringBuilder line, ClickHouseColumn column, RowValue value)
        {
            if (value.Kind == RowValueKind.Null)
            {
                line.Append("null");
                return;
            }

            var text = value.Text!;

            switch (column.Kind)
            {
                case ColumnKind.Timestamp when column.DateTime64Type is { } type && _server != null:
                    line.Append(RowBinaryValues.GetDateTime64(column, text, type, _server));
                    break;

                case ColumnKind.Date or ColumnKind.Timestamp:
                    // the text of a date or a time has nothing to escape
                    line.Append('"').Append(Clamp(column.Kind, text)).Append('"');
                    break;

                case ColumnKind.Json:
                    AppendJson(line, text, checkDuplicateKeys: !column.IsJsonb);
                    break;

                case ColumnKind.Other:
                    JsonUtils.WriteValue(line, (PgOid)column.TypeId, text);
                    break;

                default:
                    // the arrays: through their JSON
                    _json.Clear();
                    JsonUtils.WriteValue(_json, (PgOid)column.TypeId, text);

                    using (var doc = JsonDocument.Parse(_json.ToString()))
                    {
                        AppendValue(line, column, doc.RootElement, _server);
                    }
                    break;
            }
        }

        private void WriteBinaryRow(ClickHouseTable table, Dictionary<int, string>? unchanged, ulong transactionFinalLsn, ulong messageNo)
        {
            var writer = table.BinaryUpserts!;

            table.UpsertCount++;

            for (var i = 0; i < table.Columns.Count; i++)
            {
                if (unchanged != null && unchanged.TryGetValue(i, out var value))
                {
                    // read from ClickHouse, as JSON
                    using var doc = JsonDocument.Parse(value);

                    RowBinaryValues.Write(writer, table.BinaryTypes![i], table.Columns[i], doc.RootElement, _server!);
                }
                else
                {
                    RowBinaryValues.Write(writer, table.BinaryTypes![i], table.Columns[i], _row[i], _server!);
                }
            }

            RowBinaryValues.WriteVersion(writer, transactionFinalLsn, messageNo);
            writer.WriteByte(0);
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

        /// <summary>
        /// Fewer rows than this are an asynchronous insert (with UseAsyncInserts): ClickHouse collects them with others,
        /// instead of writing a part for a few rows. A larger batch is written at once: an asynchronous insert would only
        /// wait for the next flush of ClickHouse, since the output sends one insert at a time.
        /// </summary>
        internal const int AsyncInsertMaxRows = 1000;

        private async Task FlushAsync(ClickHouseTable table, CancellationToken token)
        {
            // the deletes go first: when a row is deleted and inserted with the same version, the last insert wins
            if (table.BinaryDeletes is { Length: > 0 } binaryDeletes)
            {
                await _client.ExecuteAsync(table.BinaryDeleteQuery, new RowBinaryContent(binaryDeletes), null, token, IsAsyncInsert(table.DeleteCount)).ConfigureAwait(false);

                binaryDeletes.Clear();
            }

            if (table.Deletes.Length > 0)
            {
                await _client.ExecuteAsync(table.DeleteQuery, new StringBuilderContent(table.Deletes), null, token, IsAsyncInsert(table.DeleteCount)).ConfigureAwait(false);

                table.Deletes.Clear();
            }

            table.DeleteCount = 0;

            if (table.BinaryUpserts is { Length: > 0 } binaryUpserts)
            {
                await _client.ExecuteAsync(table.BinaryUpsertQuery, new RowBinaryContent(binaryUpserts), null, token, IsAsyncInsert(table.UpsertCount)).ConfigureAwait(false);

                binaryUpserts.Clear();
            }

            if (table.Upserts.Length > 0)
            {
                await _client.ExecuteAsync(table.UpsertQuery, new StringBuilderContent(table.Upserts), null, token, IsAsyncInsert(table.UpsertCount)).ConfigureAwait(false);

                table.Upserts.Clear();
            }

            table.UpsertCount = 0;
        }

        private bool IsAsyncInsert(int rows) => _options.UseAsyncInserts && rows < AsyncInsertMaxRows;

        private async Task<ClickHouseTable> EnsureTableAsync(string tableName, string schemaName, string relationName, JsonElement root, CancellationToken token)
        {
            if (!root.TryGetProperty("s", out var schemaElement))
            {
                // the schema comes with the first row of a table on every connection
                return _tables.TryGetValue(tableName, out var cached) ? cached : throw new Exception("Missing table schema: " + tableName);
            }

            var table = ClickHouseTable.FromSchema(Names.Get(tableName, schemaName, relationName), schemaElement, _options.UseJsonType, _options.UseArrayTypes, _options.Tables.GetValueOrDefault(tableName), _timestampTimeZone);

            return await PrepareTableAsync(tableName, table, token).ConfigureAwait(false);
        }

        /// <summary>
        /// The table of a row: the same <see cref="RowTable"/> until the columns may have changed.
        /// </summary>
        private async Task<ClickHouseTable> EnsureTableAsync(RowTable rowTable, CancellationToken token)
        {
            if (_tables.TryGetValue(rowTable.Name, out var cached) && cached.Source == rowTable) return cached;

            var table = ClickHouseTable.FromRowTable(Names.Get(rowTable.Name, rowTable.SchemaName, rowTable.RelationName), rowTable, _options.UseJsonType, _options.UseArrayTypes, _options.Tables.GetValueOrDefault(rowTable.Name), _timestampTimeZone);

            return await PrepareTableAsync(rowTable.Name, table, token).ConfigureAwait(false);
        }

        private async Task<ClickHouseTable> PrepareTableAsync(string tableName, ClickHouseTable table, CancellationToken token)
        {
            // the rows that wait were written for the columns the table had before
            if (_tables.TryGetValue(tableName, out var existing))
            {
                await FlushAsync(existing, token).ConfigureAwait(false);
            }

            await _client.ExecuteAsync(table.GetCreateStatement(_options.CleanUpDeletedRowsAfter), null, null, token).ConfigureAwait(false);

            // a table created before the option was set gets the settings now
            if (table.GetCleanupStatement(_options.CleanUpDeletedRowsAfter) is { } cleanupStatement)
            {
                await _client.ExecuteAsync(cleanupStatement, null, null, token).ConfigureAwait(false);
            }

            if (table.Columns.Count > table.KeyColumns.Count)
            {
                await _client.ExecuteAsync(table.GetAlterStatement(), null, null, token).ConfigureAwait(false);
            }

            // the types the columns really have: an existing table can have others than the output would create
            var types = await GetColumnTypesAsync(table, token).ConfigureAwait(false);

            table.UseExistingTypes(types);

            // the range of its dates, also for the timestamps the JSON has as numbers
            _server ??= await GetServerInfoAsync(token).ConfigureAwait(false);

            if (_server.HasRawDateTimeNumberSetting) table.ReadDateTimeNumbersAsRawValues();

            if (_options.UseRowBinary)
            {
                if (table.UseRowBinary(types, _server.TimeZone) is { } column)
                {
                    _logger?.LogInformation("The rows of {Table} are sent as JSON: RowBinary does not cover the column {Column}", table.QuotedName, column);
                }
            }

            _logger?.LogInformation("Table {Table} is ready for {PgTable}", table.QuotedName, tableName);

            _tables[tableName] = table;

            return table;
        }

        private async Task<ClickHouseServerInfo> GetServerInfoAsync(CancellationToken token)
        {
            // an older server saturates the dates it cannot store, so these are its own first and last ones
            var result = await _client.ExecuteAsync(
                "SELECT timezone(), toInt32(toDate32('0001-01-01')), toInt32(toDate32('9999-12-31')), " +
                "toUnixTimestamp64Micro(toDateTime64('0001-01-01 00:00:00', 6, 'UTC')), toUnixTimestamp64Micro(toDateTime64('9999-12-31 23:59:59.999999', 6, 'UTC')), " +
                "(SELECT count() FROM system.settings WHERE name = 'input_format_read_datetime_number_as_raw_value') " +
                "FORMAT JSONCompactEachRow", null, null, token).ConfigureAwait(false);

            using var doc = JsonDocument.Parse(result);

            var row = doc.RootElement;

            var timeZoneName = row[0].GetString()!;
            var timeZone = ClickHouseBinaryType.FindTimeZone(timeZoneName);

            if (timeZone == null)
            {
                _logger?.LogWarning("The time zone {TimeZone} of the ClickHouse server is not known here: tables with a DateTime64 column without a time zone get JSON", timeZoneName);
            }

            static long Number(JsonElement value) => value.ValueKind == JsonValueKind.String ? long.Parse(value.GetString()!, System.Globalization.CultureInfo.InvariantCulture) : value.GetInt64();

            return new ClickHouseServerInfo(timeZone, (int)Number(row[1]), (int)Number(row[2]), Number(row[3]), Number(row[4]), Number(row[5]) > 0);
        }

        private async Task<Dictionary<string, string>> GetColumnTypesAsync(ClickHouseTable table, CancellationToken token)
        {
            var result = await _client.ExecuteAsync(
                "SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = {t:String} FORMAT JSONCompactEachRow",
                null, [new("t", EscapeParameter(table.Name))], token).ConfigureAwait(false);

            var types = new Dictionary<string, string>();

            foreach (var line in result.Split('\n', StringSplitOptions.RemoveEmptyEntries))
            {
                using var row = JsonDocument.Parse(line);

                types[row.RootElement[0].GetString()!] = row.RootElement[1].GetString()!;
            }

            return types;
        }

        private void AppendDelete(ClickHouseTable table, JsonElement keyElement, string version, ulong txFinalLsn, ulong messageNo)
        {
            if (keyElement.GetArrayLength() != table.KeyColumns.Count)
            {
                throw new Exception($"The key of {table.QuotedName} has {keyElement.GetArrayLength()} values, the schema has {table.KeyColumns.Count} key columns");
            }

            table.DeleteCount++;

            if (table.BinaryKeyTypes != null)
            {
                var writer = table.BinaryDeletes!;

                for (var i = 0; i < table.KeyColumns.Count; i++)
                {
                    RowBinaryValues.Write(writer, table.BinaryKeyTypes[i], table.KeyColumns[i], keyElement[i], _server!);
                }

                RowBinaryValues.WriteVersion(writer, txFinalLsn, messageNo);
                writer.WriteByte(1);
                return;
            }

            var line = table.Deletes.Append('[');

            for (var i = 0; i < table.KeyColumns.Count; i++)
            {
                AppendValue(line, table.KeyColumns[i], keyElement[i], _server);

                line.Append(',');
            }

            line.Append('"').Append(version).Append("\",1]\n");
        }

        internal static void AppendValue(StringBuilder line, ClickHouseColumn column, JsonElement value, ClickHouseServerInfo? server = null)
        {
            if (column.IsArray && value.ValueKind == JsonValueKind.Array)
            {
                AppendArray(line, column, value, server);
            }
            else if (column.Kind == ColumnKind.TextArray && value.ValueKind == JsonValueKind.String)
            {
                var json = new StringBuilder();

                JsonUtils.WriteArrayOfText(json, value.GetString());

                using var doc = JsonDocument.Parse(json.ToString());

                AppendArray(line, column, doc.RootElement, server);
            }
            else if (column.Kind == ColumnKind.Json && value.ValueKind == JsonValueKind.String)
            {
                AppendJson(line, value.GetString()!, checkDuplicateKeys: !column.IsJsonb);
            }
            else if (column.Kind == ColumnKind.Timestamp && column.DateTime64Type is { } type && server != null && value.ValueKind == JsonValueKind.String)
            {
                // the number ClickHouse stores: PostgreSQL's rules for a time of a time zone, which ClickHouse's parser does not follow
                line.Append(RowBinaryValues.GetDateTime64(column, value.GetString()!, type, server));
            }
            else if (column.Kind is ColumnKind.Date or ColumnKind.Timestamp && value.ValueKind == JsonValueKind.String)
            {
                line.Append('"').Append(Clamp(column.Kind, value.GetString()!)).Append('"');
            }
            else
            {
                line.Append(value.GetRawText());
            }
        }

        /// <summary>
        /// A ClickHouse array has the dimensions of its type, and PostgreSQL does not send the dimensions of a column:
        /// the type has one, and a value with more is an error that says what to do. Dates are clamped one by one.
        /// </summary>
        private static void AppendArray(StringBuilder line, ClickHouseColumn column, JsonElement value, ClickHouseServerInfo? server)
        {
            line.Append('[');

            var first = true;

            foreach (var element in value.EnumerateArray())
            {
                if (element.ValueKind == JsonValueKind.Array)
                {
                    throw new Exception($"Column {column.Name} has a multi-dimensional array, and its ClickHouse type {column.Type} has one dimension. " +
                        "Change the ClickHouse column to String (ALTER TABLE ... MODIFY COLUMN ... String), or set UseArrayTypes to false for new tables.");
                }

                if (!first) line.Append(',');
                first = false;

                if (column.Kind == ColumnKind.TimestampArray && column.DateTime64Type is { } type && server != null && element.ValueKind == JsonValueKind.String)
                {
                    line.Append(RowBinaryValues.GetDateTime64(column, element.GetString()!, type, server));
                }
                else if (column.Kind is ColumnKind.DateArray or ColumnKind.TimestampArray && element.ValueKind == JsonValueKind.String)
                {
                    line.Append('"').Append(Clamp(column.Kind == ColumnKind.DateArray ? ColumnKind.Date : ColumnKind.Timestamp, element.GetString()!)).Append('"');
                }
                else
                {
                    line.Append(element.GetRawText());
                }
            }

            line.Append(']');
        }

        // reused by the json values of a thread, one at a time
        [ThreadStatic] private static ArrayBufferWriter<byte>? t_jsonBuffer;
        [ThreadStatic] private static Utf8JsonWriter? t_jsonWriter;
        [ThreadStatic] private static char[]? t_jsonChars;

        /// <summary>
        /// The value of a json column comes as a string with the JSON text. The JSON type of ClickHouse takes it as it is,
        /// but holds only objects: any other value is stored as {"v": value}. A jsonb value has no key twice.
        /// </summary>
        internal static void AppendJson(StringBuilder line, string json, bool checkDuplicateKeys = true)
        {
            var bytes = NormalizeJson(json, checkDuplicateKeys);

            var chars = t_jsonChars is { } cached && cached.Length >= bytes.Length ? cached : t_jsonChars = new char[Math.Max(bytes.Length, 1024)];

            line.Append(chars, 0, Encoding.UTF8.GetChars(bytes, chars));
        }

        /// <summary>
        /// The UTF-8 of a json value as the JSON type of ClickHouse takes it: an object, on one line (the text of
        /// a json column can have line breaks, and a row of the text formats must be one line). Valid until the next call on the thread.
        /// </summary>
        internal static ReadOnlySpan<byte> NormalizeJson(string json, bool checkDuplicateKeys)
        {
            using var doc = JsonDocument.Parse(json);

            var buffer = t_jsonBuffer ??= new ArrayBufferWriter<byte>(1024);

            buffer.ResetWrittenCount();

            var writer = t_jsonWriter ??= new Utf8JsonWriter(buffer, new JsonWriterOptions { Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping });

            writer.Reset(buffer);

            if (doc.RootElement.ValueKind == JsonValueKind.Object)
            {
                WriteJson(writer, doc.RootElement, checkDuplicateKeys);
            }
            else
            {
                writer.WriteStartObject();
                writer.WritePropertyName("v");
                WriteJson(writer, doc.RootElement, checkDuplicateKeys);
                writer.WriteEndObject();
            }

            writer.Flush();

            return buffer.WrittenSpan;
        }

        /// <summary>
        /// Writes the value without what ClickHouse rejects in a JSON column: a key that is twice in an object
        /// (the last one wins, as in jsonb) and a number too large for a double (written as a string).
        /// </summary>
        private static void WriteJson(Utf8JsonWriter writer, JsonElement element, bool checkDuplicateKeys)
        {
            switch (element.ValueKind)
            {
                case JsonValueKind.Object:
                    writer.WriteStartObject();

                    HashSet<string>? names = null;
                    var hasDuplicates = false;

                    if (checkDuplicateKeys)
                    {
                        foreach (var property in element.EnumerateObject())
                        {
                            if (!(names ??= []).Add(property.Name)) hasDuplicates = true;
                        }
                    }

                    var index = 0;

                    foreach (var property in element.EnumerateObject())
                    {
                        index++;

                        if (hasDuplicates && IsRepeatedLater(element, property.Name, index)) continue;

                        writer.WritePropertyName(property.Name);

                        WriteJson(writer, property.Value, checkDuplicateKeys);
                    }

                    writer.WriteEndObject();
                    break;

                case JsonValueKind.Array:
                    writer.WriteStartArray();

                    foreach (var item in element.EnumerateArray())
                    {
                        WriteJson(writer, item, checkDuplicateKeys);
                    }

                    writer.WriteEndArray();
                    break;

                case JsonValueKind.Number when element.TryGetDouble(out var number) && double.IsInfinity(number):
                    writer.WriteStringValue(element.GetRawText());
                    break;

                default:
                    element.WriteTo(writer);
                    break;
            }
        }

        private static bool IsRepeatedLater(JsonElement element, string name, int skip)
        {
            foreach (var property in element.EnumerateObject())
            {
                if (skip-- > 0) continue;

                if (property.NameEquals(name)) return true;
            }

            return false;
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

        private static bool IsSameKey(ClickHouseTable table, JsonElement keyElement, JsonElement[] rowElement)
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
        private async Task<Dictionary<int, string>?> GetUnchangedValuesAsync(ClickHouseTable table, JsonElement[] rowElement, JsonElement? keyElement, CancellationToken token)
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

            if (table.Engine == ClickHouseTableEngine.Null)
            {
                if (!table.UnchangedValueWarned)
                {
                    table.UnchangedValueWarned = true;

                    _logger?.LogWarning("An update of {Table} did not change a large value, and a table with the Null engine has no row to take it from. " +
                        "The value is null, now and in later updates like this. With REPLICA IDENTITY FULL PostgreSQL sends it.", table.QuotedName);
                }

                return values;
            }

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

                if (column.Kind == ColumnKind.Timestamp && column.DateTime64Type is { } type && _server != null)
                {
                    // the moment, as the row has it: the text would be parsed by ClickHouse's rules
                    conditions.Add($"{column.QuotedName} = fromUnixTimestamp64Micro({{k{keyIndex}:Int64}})");
                    parameters.Add(new($"k{keyIndex}", RowBinaryValues.GetDateTime64Micros(column, text, type, _server).ToString(System.Globalization.CultureInfo.InvariantCulture)));
                    keyIndex++;
                    continue;
                }

                if (column.Kind is ColumnKind.Date or ColumnKind.Timestamp) text = Clamp(column.Kind, text);

                conditions.Add($"{column.QuotedName} = {{k{keyIndex}:{column.Type}}}");
                parameters.Add(new($"k{keyIndex}", EscapeParameter(text)));

                keyIndex++;
            }

            // the latest version, unless it is a delete: without FINAL, which a MergeTree table does not have
            var columns = string.Join(", ", missing.Select(i => table.Columns[i].QuotedName));

            var query = $"SELECT {columns} FROM (SELECT {columns}, {ClickHouseTable.DeletedColumn} FROM {table.QuotedName} " +
                        $"WHERE {string.Join(" AND ", conditions)} ORDER BY {ClickHouseTable.VersionColumn} DESC LIMIT 1) " +
                        $"WHERE {ClickHouseTable.DeletedColumn} = 0 FORMAT JSONCompactEachRow";

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
