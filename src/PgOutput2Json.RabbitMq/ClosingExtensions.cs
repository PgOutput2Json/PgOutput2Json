using RabbitMQ.Client;

using PgOutput2Json.Core;

namespace PgOutput2Json.RabbitMq
{
    internal static class ClosingExtensions
    {
        /// <summary>
        /// Calls Dispose on disposable object. If exception is thrown during dispose - it is logged as Warn and ignored.
        /// </summary>
        /// <param name="disposable"></param>
        public static void TryDispose(this IDisposable? disposable, LoggingErrorHandler? logger)
        {
            try
            {
                disposable?.Dispose();
            }
            catch (Exception ex)
            {
                logger?.Invoke(ex, "Error disposing a disposable object");
            }
        }

        public static void TryClose(this IConnection? connection, LoggingErrorHandler? logger)
        {
            try
            {
                connection?.Close();
            }
            catch (Exception ex)
            {
                logger?.Invoke(ex, "Error closing RabbitMq connection");
            }
        }
    }
}
