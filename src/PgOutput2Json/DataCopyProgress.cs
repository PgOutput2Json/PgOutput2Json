using Npgsql;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace PgOutput2Json
{
    internal static class DataCopyProgress
    {
        public static async Task<DataCopyStatus> GetDataCopyStatus(string tableName, ReplicationListenerOptions listenerOptions, CancellationToken token)
        {
            var (DatabaseName, SlotName, ConnectionString) = GetPgoutput2JsonInfo(listenerOptions);

            if (SlotName == null) return new DataCopyStatus {  IsCompleted = false }; // temporary slot, no use remembering where we left off

            using var cn = new NpgsqlConnection(ConnectionString);

            await cn.OpenAsync(token).ConfigureAwait(false);

            using var cmd = cn.CreateCommand();

            cmd.CommandText = @"
SELECT is_completed, last_message 
FROM data_copy_progress 
WHERE database_name = @database_name
    AND table_name = @table_name
    AND slot_name = @slot_name
";
            cmd.Parameters.AddWithValue("database_name", DatabaseName);
            cmd.Parameters.AddWithValue("table_name", tableName);
            cmd.Parameters.AddWithValue("slot_name", SlotName);

            using var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);

            var result = new DataCopyStatus();

            if (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                result.IsCompleted = reader.GetBoolean(0);
                result.LastJson = reader.IsDBNull(1) ? null : reader.GetString(1);
            }

            await reader.CloseAsync().ConfigureAwait(false);

            return result;
        }

        public static async Task SetDataCopyProgress(string tableName, ReplicationListenerOptions listenerOptions, bool isCompleted, string? lastJson, CancellationToken token)
        {
            var (DatabaseName, SlotName, ConnectionString) = GetPgoutput2JsonInfo(listenerOptions);

            if (SlotName == null) return;

            using var cn = new NpgsqlConnection(ConnectionString);

            await cn.OpenAsync(token).ConfigureAwait(false);


            using var cmd = cn.CreateCommand();

            cmd.CommandText = @"
INSERT INTO data_copy_progress (database_name, table_name, slot_name, is_completed, last_message)
VALUES (@database_name, @table_name, @slot_name, @is_completed, @last_message)
ON CONFLICT (database_name, table_name, slot_name)
DO UPDATE SET
    is_completed = EXCLUDED.is_completed,
    last_message = EXCLUDED.last_message
";
            cmd.Parameters.AddWithValue("is_completed", isCompleted);
            cmd.Parameters.AddWithValue("last_message", lastJson ?? (object)DBNull.Value);

            cmd.Parameters.AddWithValue("database_name", DatabaseName);
            cmd.Parameters.AddWithValue("table_name", tableName);
            cmd.Parameters.AddWithValue("slot_name", SlotName);

            await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }

        public static async Task CreateDataCopyProgressTable(ReplicationListenerOptions listenerOptions, CancellationToken token)
        {
            var (DatabaseName, SlotName, ConnectionString) = GetPgoutput2JsonInfo(listenerOptions);

            if (SlotName == null) return;

            using var cn = new NpgsqlConnection(ConnectionString);

            await cn.OpenAsync(token).ConfigureAwait(false);

            using var cmd = cn.CreateCommand();

            cmd.CommandText = @"
CREATE TABLE IF NOT EXISTS data_copy_progress (
    table_name TEXT NOT NULL,
    database_name TEXT NOT NULL,
    slot_name TEXT NOT NULL,
    is_completed bool NOT NULL DEFAULT false,
    last_message TEXT,
    CONSTRAINT pk_data_copy_progress PRIMARY KEY (table_name, database_name, slot_name)
)";

            await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }

        private static (string DatabaseName, string? SlotName, string ConnectionString) GetPgoutput2JsonInfo(ReplicationListenerOptions listenerOptions)
        {
            string? slotName;
            if (string.IsNullOrWhiteSpace(listenerOptions.ReplicationSlotName) || listenerOptions.UseTemporarySlot)
            {
                slotName = null;
            }
            else
            {
                slotName = listenerOptions.ReplicationSlotName;
            }

            var cnBuilder = new NpgsqlConnectionStringBuilder(listenerOptions.ConnectionString);

            var databaseName = cnBuilder.Database ?? throw new Exception("Database name not set in connection string");

            cnBuilder.Database = "pg_output2json";
            var ourConnectionString = cnBuilder.ConnectionString;

            return (databaseName, slotName, ourConnectionString);
        }
    }
}
