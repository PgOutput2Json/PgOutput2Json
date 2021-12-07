using RabbitMQ.Client;

using PgOutput2Json.Core;

namespace PgOutput2Json.RabbitMq
{
    public static class ClosingExtensions
    {
        public static void TryClose(this IConnection? connection, LoggingErrorHandler? logger)
        {
            try
            {
                connection?.Close();
            }
            catch (Exception ex)
            {
                try
                {
                    logger?.Invoke(ex, "Error closing RabbitMq connection");
                }
                catch
                {
                }
            }
        }
    }
}
