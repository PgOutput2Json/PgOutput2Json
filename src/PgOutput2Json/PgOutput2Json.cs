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

        public async Task Start(CancellationToken cancellationToken)
        {
            lock (_lock)
            {
                if (_cancellationTokenSource != null) return; // already running
                _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            }

            try
            {
                await _listener.ListenForChanges(_cancellationTokenSource.Token);
            }
            finally
            {
                Dispose();
            }
        }

        public void Dispose()
        {
            lock (_lock)
            {
                _cancellationTokenSource?.Cancel();
                _cancellationTokenSource?.TryDispose(_logger);
                _cancellationTokenSource = null;
            }
        }
    }
}
