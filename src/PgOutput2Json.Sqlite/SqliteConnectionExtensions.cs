using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Data.Sqlite;

namespace PgOutput2Json.Sqlite
{
    internal static class SqliteConnectionExtensions
    {
        public static async Task<ulong> GetWalEnd(this SqliteConnection cn, CancellationToken token)
        {
            var cfgValue = await GetConfig(cn, ConfigKey.WalEnd, token).ConfigureAwait(false);
            
            return cfgValue != null ? ulong.Parse(cfgValue, CultureInfo.InvariantCulture) : 0;
        }

        public static async Task SetWalEnd(this SqliteConnection cn, ulong walEnd, CancellationToken token)
        {
            await SaveConfig(cn, ConfigKey.WalEnd, walEnd.ToString(CultureInfo.InvariantCulture), token).ConfigureAwait(false);
        }

        public static async Task SaveConfig(this SqliteConnection cn, string key, string value, CancellationToken token)
        {
            using var cmd = cn.CreateCommand();

            cmd.Parameters.AddWithValue(CommandText.ConfigColKey, key);
            cmd.Parameters.AddWithValue(CommandText.ConfigColValue, value);

            cmd.CommandText = CommandText.ConfigUpdate;

            var result = await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);

            if (result == 0)
            {
                cmd.CommandText = CommandText.ConfigInsert;

                await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
            }
        }

        public static async Task<string?> GetConfig(this SqliteConnection cn, string key, CancellationToken token)
        {
            using var cmd = cn.CreateCommand();

            cmd.Parameters.AddWithValue(CommandText.ConfigColKey, key);

            cmd.CommandText = CommandText.ConfigSelect;

            var result = await cmd.ExecuteScalarAsync(token).ConfigureAwait(false);

            return result?.ToString();
        }
    }
}
