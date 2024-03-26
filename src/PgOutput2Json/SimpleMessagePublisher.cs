using Microsoft.Extensions.Logging;

namespace PgOutput2Json
{
    public delegate void SimpleMessageHandler(string json, string tableName, string keyColumnValue, int partition);

    internal class SimpleMessagePublisherFactory : IMessagePublisherFactory
    {
        private readonly SimpleMessageHandler _messageHandler;

        public SimpleMessagePublisherFactory(SimpleMessageHandler messageHandler)
        {
            _messageHandler = messageHandler;
        }

        public IMessagePublisher CreateMessagePublisher(ILoggerFactory? loggerFactory)
        {
            return new SimpleMessagePublisher(_messageHandler, loggerFactory?.CreateLogger<SimpleMessagePublisher>());
        }
    }

    internal class SimpleMessagePublisher : IMessagePublisher
    {
        private readonly SimpleMessageHandler _messageHandler;
        private readonly ILogger<SimpleMessagePublisher>? _logger;

        public SimpleMessagePublisher(SimpleMessageHandler messageHandler, ILogger<SimpleMessagePublisher>? logger)
        {
            _messageHandler = messageHandler;
            _logger = logger;
        }

        public bool Publish(string json, string tableName, string keyColumnValue, int partition)
        {
            _logger?.LogDebug("Message for {Table}: {Json}", tableName, json);

            _messageHandler.Invoke(json, tableName, keyColumnValue, partition);

            return true;
        }

        public void ForceConfirm()
        {
        }

        public void Dispose()
        {
        }
    }
}
