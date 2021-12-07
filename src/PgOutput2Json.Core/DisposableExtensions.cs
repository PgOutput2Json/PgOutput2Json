namespace PgOutput2Json.Core
{
    public static class DisposableExtensions
    {
        public static void TryDispose(this IDisposable? disposable, LoggingErrorHandler? logger)
        {
            try
            {
                disposable?.Dispose();
            }
            catch (Exception ex)
            {
                try
                {
                    logger?.Invoke(ex, "Error disposing a disposable object");
                }
                catch
                {
                }
            }
        }

        public static ValueTask TryDisposeAsync(this IAsyncDisposable? disposable, LoggingErrorHandler? logger)
        {
            try
            {
                if (disposable != null)
                {
                    return disposable.DisposeAsync();
                }
            }
            catch (Exception ex)
            {
                try
                {
                    logger?.Invoke(ex, "Error disposing a disposable object");
                }
                catch
                {
                }
            }

            return ValueTask.CompletedTask;
        }
    }
}
