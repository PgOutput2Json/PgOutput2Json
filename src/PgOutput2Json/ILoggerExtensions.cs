using Microsoft.Extensions.Logging;
using System;

namespace PgOutput2Json
{
    static class ILoggerExtensions
    {
        public static void SafeLogDebug(this ILogger? logger, string message)
        {
            try
            {
                if (logger != null && logger.IsEnabled(LogLevel.Debug))
                {
                    logger.LogDebug(message);
                }
            }
            catch
            {
            }
        }

        public static void SafeLogInfo(this ILogger? logger, string message)
        {
            try
            {
                if (logger != null && logger.IsEnabled(LogLevel.Information))
                {
                    logger.LogInformation(message);
                }
            }
            catch
            {
            }
        }

        public static void SafeLogWarn(this ILogger? logger, string message)
        {
            try
            {
                if (logger != null && logger.IsEnabled(LogLevel.Warning))
                {
                    logger.LogWarning(message);
                }
            }
            catch
            {
            }
        }

        public static void SafeLogError(this ILogger? logger, Exception ex, string message)
        {
            try
            {
                if (logger != null && logger.IsEnabled(LogLevel.Error))
                {
                    logger.LogError(ex, message);
                }
            }
            catch
            {
            }
        }
    }
}
