using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace PgOutput2Json
{
    internal static class DataCopyProgress
    {
        public static async Task<DataCopyStatus> GetDataCopyStatus(string tableName, ReplicationListenerOptions listenerOptions, CancellationToken token)
        {
            listenerOptions.OrderedKeys.TryGetValue(tableName, out var orderByKeys);

            var result = new DataCopyStatus
            {
                IsCompleted = false,
                OrderByColumns = orderByKeys != null ? string.Join(',', orderByKeys.Select(k => $"\"{k}\"")) : null,
            };

            var slotName = GetSlotNameForDataCopyProgress(listenerOptions);

            if (slotName == null)
            {
                // temporary slot, no use remembering where we left off
                return result;
            }

            using var cn = new NpgsqlConnection(listenerOptions.ConnectionString);

            await cn.OpenAsync(token).ConfigureAwait(false);

            using var cmd = cn.CreateCommand();

            cmd.CommandText = @"
SELECT is_completed, last_message, column_names
FROM pgoutput2json.data_copy_progress 
WHERE table_name = @table_name
    AND slot_name = @slot_name
";
            cmd.Parameters.AddWithValue("table_name", tableName);
            cmd.Parameters.AddWithValue("slot_name", slotName);

            using var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);

            string? lastMessage = null;
            string? columnNames = null;

            if (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                result.IsCompleted = reader.GetBoolean(0);

                lastMessage = reader.IsDBNull(1) ? null : reader.GetString(1);
                columnNames = reader.IsDBNull(2) ? null : reader.GetString(2);
            }

            result.AdditionalRowFilter = GetRowFilter(tableName, orderByKeys, columnNames, lastMessage);

            await reader.CloseAsync().ConfigureAwait(false);

            return result;
        }

        private static string? GetRowFilter(string tableName, IReadOnlyList<string>? orderByKeys, string? columNames, string? lastMessage)
        {
            if (lastMessage == null || orderByKeys == null || orderByKeys.Count == 0)
            {
                return null;
            }

            var doc = JsonDocument.Parse(lastMessage);
            if (!doc.RootElement.TryGetProperty("r", out var rowElement)) throw new Exception($"Missing row element in the last message stored for table {tableName}");

            var lastSeenValues = new List<(string Column, string Value)>();

            if (rowElement.ValueKind == JsonValueKind.Array)
            {
                // compact mode
                if (columNames == null)
                {
                    throw new Exception($"Column names not found for table {tableName} when trying to get data copy status");
                }

                var cols = JsonSerializer.Deserialize<List<string>>(columNames) ?? throw new Exception("Could not deserialize column names");

                for (int i = 0; i < orderByKeys.Count; i++)
                {
                    var colIndex = cols.FindIndex(c => c == orderByKeys[i]);

                    if (colIndex < 0) 
                    {
                        throw new Exception($"Column for the provided ordered key '{orderByKeys[i]}' not found in table {tableName}");
                    }

                    if (colIndex >= rowElement.GetArrayLength())
                    {
                        throw new Exception($"Too few values in the last message stored for table {tableName}. Expected at least {colIndex + 1} values");
                    }

                    lastSeenValues.Add((orderByKeys[i], FormatValue(rowElement[colIndex])));
                }
            }
            else
            {
                // default mode
                for (int i = 0; i < orderByKeys.Count; i++)
                {
                    if (!rowElement.TryGetProperty(orderByKeys[i], out var valueElement))
                    {
                        throw new Exception($"Property for the provided ordered key {orderByKeys[i]} not found in the last message stored for table {tableName}.");
                    }

                    lastSeenValues.Add((orderByKeys[i], FormatValue(valueElement)));
                }
            }

            var strCols = string.Join(',', lastSeenValues.Select(x => $"\"{x.Column}\""));
            var strVals = string.Join(',', lastSeenValues.Select(x => x.Value));

            return $"({strCols}) > ({strVals})";
        }

        private static string FormatValue(JsonElement jsonElement)
        {
            return jsonElement.ValueKind switch
            {
                JsonValueKind.String => $"'{jsonElement.GetString()}'",
                JsonValueKind.Number => $"{jsonElement}",
                JsonValueKind.True => "true",
                JsonValueKind.False => "false",
                _ => $"'{jsonElement.GetRawText()}'",
            };
        }

        public static async Task SetDataCopyProgress(string tableName,
                                                     ReplicationListenerOptions listenerOptions,
                                                     bool isCompleted,
                                                     string? lastMessage,
                                                     IReadOnlyList<PgColumnInfo>? columns,
                                                     CancellationToken token)
        {
            var slotName = GetSlotNameForDataCopyProgress(listenerOptions);

            if (slotName == null) return;

            using var cn = new NpgsqlConnection(listenerOptions.ConnectionString);

            await cn.OpenAsync(token).ConfigureAwait(false);

            using var cmd = cn.CreateCommand();

            cmd.CommandText = @"
INSERT INTO pgoutput2json.data_copy_progress (table_name, slot_name, is_completed, last_message, column_names, updated_at)
VALUES (@table_name, @slot_name, @is_completed, @last_message::jsonb, @column_names::jsonb, now())
ON CONFLICT (table_name, slot_name)
DO UPDATE SET
    is_completed = EXCLUDED.is_completed,
    last_message = EXCLUDED.last_message,
    column_names = EXCLUDED.column_names,
    updated_at = EXCLUDED.updated_at
";
            cmd.Parameters.AddWithValue("table_name", tableName);
            cmd.Parameters.AddWithValue("slot_name", slotName);

            cmd.Parameters.AddWithValue("is_completed", isCompleted);
            cmd.Parameters.AddWithValue("last_message", lastMessage ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue("column_names", columns != null ? JsonSerializer.Serialize(columns.Select(c => c.ColumnName)) : DBNull.Value);

            await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }

        public static async Task CreateDataCopyProgressTable(ReplicationListenerOptions listenerOptions, CancellationToken token)
        {
            if (!ShouldTrackProgress(listenerOptions)) return; // no need to create table if no progress tracking

            using var cn = new NpgsqlConnection(listenerOptions.ConnectionString);

            await cn.OpenAsync(token).ConfigureAwait(false);

            using var cmd = cn.CreateCommand();

            cmd.CommandText = @"
CREATE TABLE IF NOT EXISTS pgoutput2json.data_copy_progress (
    table_name TEXT NOT NULL,
    slot_name TEXT NOT NULL,
    is_completed bool NOT NULL DEFAULT false,
    last_message jsonb,
    column_names jsonb,
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now(),
    CONSTRAINT pk_data_copy_progress PRIMARY KEY (table_name, slot_name)
)";

            await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }

        private static string? GetSlotNameForDataCopyProgress(ReplicationListenerOptions listenerOptions)
        {
            return ShouldTrackProgress(listenerOptions) 
                ? listenerOptions.ReplicationSlotName 
                : null;
        }

        private static bool ShouldTrackProgress(ReplicationListenerOptions listenerOptions)
        {
            // we don't store progress for temporary slots
            return !string.IsNullOrWhiteSpace(listenerOptions.ReplicationSlotName) && !listenerOptions.UseTemporarySlot;
        }
    }
}
