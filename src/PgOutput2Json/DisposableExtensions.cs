using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace PgOutput2Json
{
    static class DisposableExtensions
    {
        public static void TryDispose(this IDisposable? disposable, ILogger? logger)
        {
            try
            {
                disposable?.Dispose();
            }
            catch (Exception ex)
            {
                try
                {
                    logger?.LogError(ex, "Error disposing a disposable object");
                }
                catch
                {
                }
            }
        }

        public static void TryDispose(this IEnumerable<IDisposable>? disposables, ILogger? logger)
        {
            if (disposables == null) return;

            foreach (var disposable in disposables)
            {
                try
                {
                    disposable?.Dispose();
                }
                catch (Exception ex)
                {
                    try
                    {
                        logger?.LogError(ex, "Error disposing a disposable object");
                    }
                    catch
                    {
                    }
                }
            }
        }

        public static ValueTask TryDisposeAsync(this IAsyncDisposable? disposable, ILogger? logger)
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
                    logger?.LogError(ex, "Error disposing a disposable object");
                }
                catch
                {
                }
            }

#if NETSTANDARD2_1
            return new ValueTask();
#else
            return ValueTask.CompletedTask;
#endif
        }

        public static void TryCancel(this CancellationTokenSource? tokenSource, ILogger? logger)
        {
            try
            {
                tokenSource?.Cancel();
            }
            catch (Exception ex)
            {
                try
                {
                    logger?.LogError(ex, "Error cancelling a token source");
                }
                catch
                {
                }
            }
        }
    }
}
