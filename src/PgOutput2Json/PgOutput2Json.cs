using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace PgOutput2Json
{
    internal sealed class PgOutput2Json : IPgOutput2Json
    {
        private readonly ReplicationListener _listener;
        private readonly ILogger<PgOutput2Json>? _logger;

        private CancellationTokenSource? _cancellationTokenSource;

        private readonly object _lock = new object();

        public PgOutput2Json(ReplicationListener listener, ILoggerFactory? loggerFactory)
        {
            _listener = listener;
            _logger = loggerFactory?.CreateLogger<PgOutput2Json>();
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            lock (_lock)
            {
                if (_cancellationTokenSource != null) return; // already running
                _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            }

            try
            {
                await _listener.ListenForChangesAsync(_cancellationTokenSource.Token).ConfigureAwait(false);
            }
            finally
            {
                Dispose();
            }
        }

        /*
        public Task<bool> WhenReplicationStartsAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            return WhenLsnReachesAsync("0/0", timeout, cancellationToken);
        }

        public async Task<bool> WhenLsnReachesAsync(string expectedLsn, TimeSpan timeout, CancellationToken cancellationToken)
        {
            using var timeoutCts = new CancellationTokenSource(timeout);
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

            try
            {
                await _listener.WhenLsnReachesAsync(expectedLsn, linkedCts.Token).ConfigureAwait(false);
                return true;
            }
            catch (OperationCanceledException)
            {
                if (timeoutCts.Token.IsCancellationRequested) return false;
                throw;
            }
        }
        */

        public void Dispose()
        {
            lock (_lock)
            {
                _cancellationTokenSource.TryCancel(_logger);
                _cancellationTokenSource.TryDispose(_logger);
                _cancellationTokenSource = null;
            }
        }
    }
}
