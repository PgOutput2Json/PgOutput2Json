using Microsoft.Extensions.Logging;

namespace PgOutput2Json
{
    internal class PgOutput2Json : IPgOutput2Json
    {
        private readonly ReplicationListener _listener;
        private readonly IMessagePublisher _publisher;
        private readonly int _batchSize;
        private readonly ILogger<PgOutput2Json>? _logger;
        private readonly TimeSpan _confirmTimeout;

        //private readonly Random _rnd = new Random();

        private int _currentBatchSize = 0;

        public PgOutput2Json(ReplicationListener listener,
                             IMessagePublisher publisher,
                             int batchSize = 100,
                             int confirmTimeoutSec = 30,
                             ILogger<PgOutput2Json>? logger = null)
        {
            _listener = listener;
            _publisher = publisher;
            _batchSize = batchSize;
            _logger = logger;
            _confirmTimeout = TimeSpan.FromSeconds(confirmTimeoutSec);
            _listener.MessageHandler += MessageHandler;
            _listener.ConfirmHandler += ConfirmHandler;
        }

        public Task Start(CancellationToken cancellationToken)
        {
            return _listener.ListenForChanges(cancellationToken);
        }

        public void Dispose()
        {
            _listener.MessageHandler -= MessageHandler;
            _listener.ConfirmHandler -= ConfirmHandler;

            _listener.TryDispose(_logger);
            _publisher.TryDispose(_logger);
        }

        private void MessageHandler(string json, string tableName, string keyColumnValue, int partition, ref bool confirm)
        {
            //Console.WriteLine(keyColumnValue);

            _publisher.Publish(json, tableName, keyColumnValue, partition);

            if (++_currentBatchSize >= _batchSize)
            {
                WaitForConfirms();
                confirm = true;
            }
        }

        private void ConfirmHandler()
        {
            //if (_rnd.Next(100) <= 33) throw new Exception("Fake commit exception");
            WaitForConfirms();
        }

        private void WaitForConfirms()
        {
            if (_currentBatchSize > 0)
            {
                _publisher.WaitForConfirmsOrDie(_confirmTimeout);
                _currentBatchSize = 0;
            }
        }
    }
}
