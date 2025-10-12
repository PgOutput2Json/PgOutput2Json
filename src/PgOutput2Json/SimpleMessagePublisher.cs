using Microsoft.Extensions.Logging;
using System.Threading;
using System.Threading.Tasks;

namespace PgOutput2Json
{
    public delegate Task<bool> SimpleMessageHandler(string json, string tableName, string keyColumnValue, string partitionKey);

    internal class SimpleMessagePublisherFactory : IMessagePublisherFactory
    {
        private readonly SimpleMessageHandler _messageHandler;

        public SimpleMessagePublisherFactory(SimpleMessageHandler messageHandler)
        {
            _messageHandler = messageHandler;
        }

        public IMessagePublisher CreateMessagePublisher(ReplicationListenerOptions listenerOptions, string slotName, ILoggerFactory? loggerFactory)
        {
            return new SimpleMessagePublisher(_messageHandler, loggerFactory?.CreateLogger<SimpleMessagePublisher>());
        }
    }

    internal class SimpleMessagePublisher : MessagePublisher
    {
        private readonly SimpleMessageHandler _messageHandler;
        private readonly ILogger<SimpleMessagePublisher>? _logger;

        public SimpleMessagePublisher(SimpleMessageHandler messageHandler, ILogger<SimpleMessagePublisher>? logger)
        {
            _messageHandler = messageHandler;
            _logger = logger;
        }

        public override Task PublishAsync(JsonMessage message, CancellationToken token) 
        {
            if (_logger != null && _logger.IsEnabled(LogLevel.Debug))
            {
                _logger?.LogDebug("Message for {Table}: {Json}", message.TableName.ToString(), message.Json.ToString());
            }

            return _messageHandler.Invoke(message.Json.ToString(), message.TableName.ToString(), message.KeyKolValue.ToString(), message.PartitionKolValue.ToString());
        }

        public override Task ConfirmAsync(CancellationToken token)
        {
            return Task.CompletedTask;
        }

        public override ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }
}
