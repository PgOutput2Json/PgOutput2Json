using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace PgOutput2Json.RabbitMq
{
    public static class ClosingExtensions
    {
        public static void TryClose(this IConnection? connection, ILogger? logger)
        {
            try
            {
                connection?.Close();
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
