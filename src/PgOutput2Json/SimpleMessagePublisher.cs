﻿using Microsoft.Extensions.Logging;
using System.Threading;
using System.Threading.Tasks;

namespace PgOutput2Json
{
    public delegate Task<bool> SimpleMessageHandler(string json, string tableName, string keyColumnValue, int partition);

    internal class SimpleMessagePublisherFactory : IMessagePublisherFactory
    {
        private readonly SimpleMessageHandler _messageHandler;

        public SimpleMessagePublisherFactory(SimpleMessageHandler messageHandler)
        {
            _messageHandler = messageHandler;
        }

        public IMessagePublisher CreateMessagePublisher(ReplicationListenerOptions listenerOptions, ILoggerFactory? loggerFactory)
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

        public override Task PublishAsync(ulong walSeqNo, string json, string tableName, string keyColumnValue, int partition, CancellationToken token) 
        {
            _logger?.LogDebug("Message for {Table}: {Json}", tableName, json);

            return _messageHandler.Invoke(json, tableName, keyColumnValue, partition);
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
