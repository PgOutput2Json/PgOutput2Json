using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

using Microsoft.Data.Sqlite;

namespace PgOutput2Json.Sqlite
{
    public class SqlitePublisher : IMessagePublisher
    {
        private readonly SqlitePublisherOptions _options;
        private readonly ReplicationListenerOptions _listenerOptions;
        private readonly ILogger<SqlitePublisher>? _logger;

        private SqliteConnection? _connection;

        public SqlitePublisher(SqlitePublisherOptions options,
                               ReplicationListenerOptions listenerOptions,
                               ILogger<SqlitePublisher>? logger)
        {
            _options = options;
            _listenerOptions = listenerOptions;
            _logger = logger;
        }

        public Task PublishAsync(ulong walSeqNo, string json, string tableName, string keyColumnValue, int partition, CancellationToken token)
        {
            return Task.CompletedTask;
        }

        public Task ConfirmAsync(CancellationToken token)
        {
            return Task.CompletedTask;
        }

        public async Task<ulong> GetLastPublishedWalSeq(CancellationToken token)
        {
            var cn = await EnsureConnection(token).ConfigureAwait(false);

            return await cn.GetWalEnd(token).ConfigureAwait(false);
        }

        public async ValueTask DisposeAsync()
        {
            if (_connection != null)
            {
                try
                {
                    await _connection.DisposeAsync();
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Failed disposing Sqlite connection");
                }
            }
        }

        private async Task<SqliteConnection> EnsureConnection(CancellationToken token)
        {
            if (_connection != null) return _connection;

            _connection = new SqliteConnection(_options.ConnectionStringBuilder.ConnectionString);

            await _connection.OpenAsync(token).ConfigureAwait(false);

            using var cmd = _connection.CreateCommand();

            cmd.CommandText = CommandText.ConfigCreate;

            await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);

            return _connection;
        }
    }
}
