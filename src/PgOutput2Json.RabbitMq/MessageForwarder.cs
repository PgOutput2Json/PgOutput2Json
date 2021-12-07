using System.Text;

using PgOutput2Json.Core;

namespace PgOutput2Json.RabbitMq
{
    public class MessageForwarder: IDisposable
    {
        private readonly ReplicationListener _listener;
        private readonly MessagePublisher _publisher;
        private readonly int _batchSize;
        private readonly TimeSpan _confirmTimeout;

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
            _listener.CommitHandler += CommitHandler;
        }

        private void CommitHandler()
        {
            WaitForConfirms();
        }

        public Task Start(CancellationToken cancellationToken)
        {
            return _listener.ListenForChanges(cancellationToken);
        }

        public void Dispose()
        {
            _listener.MessageHandler -= MessageHandler;
        }

        private void MessageHandler(StringBuilder json, StringBuilder tableName, int partition, ref bool confirm)
        {
            var messageType = tableName.ToString();

            tableName.Append('.');
            tableName.Append(partition);

            var routingKey = tableName.ToString();

            var bodyString = json.ToString();

            Console.WriteLine(bodyString);

            var body = Encoding.UTF8.GetBytes(bodyString);

            _publisher.PublishMessage(body, messageType, routingKey);

            if (++_currentBatchSize >= _batchSize)
            {
                WaitForConfirms();
                confirm = true;
            }
            else
            {
                confirm = false;
            }
        }

        private void WaitForConfirms()
        {
            if (_currentBatchSize > 0)
            {
                _publisher.WaitForConfirmsOrDie(_confirmTimeout);
                _currentBatchSize = 0;

                Console.WriteLine("Confirmed");
            }
        }
    }
}
