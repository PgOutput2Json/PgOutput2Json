using PgOutput2Json.Core;

namespace PgOutput2Json.RabbitMq
{
    public class MessageForwarder: IDisposable
    {
        private readonly ReplicationListener _listener;
        private readonly MessagePublisher _publisher;
        private readonly int _batchSize;
        private readonly TimeSpan _confirmTimeout;

        //private readonly Random _rnd = new Random();

        private int _currentBatchSize = 0;

        public MessageForwarder(ReplicationListener listener,
                                MessagePublisher publisher,
                                int batchSize = 100,
                                int confirmTimeoutSec = 30)
        {
            _listener = listener;
            _publisher = publisher;
            _batchSize = batchSize;
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
        }

        private void MessageHandler(string json, string tableName, string keyColumnValue, int partition, ref bool confirm)
        {
            //Console.WriteLine(keyColumnValue);

            _publisher.PublishMessage(json, tableName, tableName + "." + partition);

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
