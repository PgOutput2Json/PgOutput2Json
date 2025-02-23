using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace PgOutput2Json.RabbitMq
{
    static class ClosingExtensions
    {
        public static async ValueTask TryCloseAsync(this IConnection? connection, ILogger? logger)
        {
            if (connection == null) return;

            try
            {
                await connection.CloseAsync()
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                try
                {
                    logger?.LogError(ex, "Error closing RabbitMq connection");
                }
                catch
                {
                }
            }
        }
    }
}
