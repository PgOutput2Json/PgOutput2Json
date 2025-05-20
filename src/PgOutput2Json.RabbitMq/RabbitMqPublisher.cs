using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace PgOutput2Json.RabbitMq
{
    public class RabbitMqPublisher: MessagePublisher
    {
        public RabbitMqPublisher(RabbitMqPublisherOptions options, int batchSize, ILogger<RabbitMqPublisher>? logger = null)
        {
            _options = options;
            _batchSize = batchSize;
            _logger = logger;
        }

        public override async Task PublishAsync(JsonMessage msg, CancellationToken token)
        {
            var channel = await EnsureConnectionAsync(token)
                .ConfigureAwait(false);

            var tableName = msg.TableName.ToString();
            var routingKey = tableName + "." + msg.Partition;
            var json = msg.Json.ToString();

            if (_options.PersistencyConfigurationByTable.Count == 0 
                || !_options.PersistencyConfigurationByTable.TryGetValue(tableName, out var persistent))
            {
                persistent = _options.UsePersistentMessagesByDefault;
            }
            
            _logger.SafeLogDebug($"Publishing to Exchange={_options.ExchangeName}, RoutingKey={routingKey}, Body={json}");

            var body = Encoding.UTF8.GetBytes(json);

            var basicProperties = new BasicProperties { Type = tableName, Persistent = persistent };

            var task = channel.BasicPublishAsync(_options.ExchangeName, routingKey, false, basicProperties, body, token);

            _pendingTasks.Add(task);
        }

        public override async Task ConfirmAsync(CancellationToken token)
        {
            foreach (var pt in _pendingTasks)
            {
                await pt.ConfigureAwait(false);
            }

            _pendingTasks.Clear();
        }

        public override async ValueTask DisposeAsync()
        {
            _pendingTasks.Clear();

            await _channel.TryDisposeAsync(_logger)
                .ConfigureAwait(false);

            if (_connection != null)
            {
                try
                {
                    await _connection.CloseAsync()
                        .ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.SafeLogError(ex, "Error closing RabbitMq connection");
                }
            }

            await _connection.TryDisposeAsync(_logger)
                .ConfigureAwait(false);
        }

        private async ValueTask<IChannel> EnsureConnectionAsync(CancellationToken token)
        {
            if (_channel != null) return _channel;

            _logger.SafeLogInfo("Connecting to RabbitMQ");

            _connection = await _options.ConnectionFactory.CreateConnectionAsync(_options.HostNames, token)
                .ConfigureAwait(false);

            _connection.CallbackExceptionAsync += ConnectionOnCallbackExceptionAsync;
            _connection.ConnectionBlockedAsync += ConnectionOnConnectionBlockedAsync;
            _connection.ConnectionUnblockedAsync += ConnectionOnConnectionUnblockedAsync;
            _connection.ConnectionShutdownAsync += ConnectionOnConnectionShutdownAsync;

            _logger.SafeLogInfo("Connected to RabbitMQ");

            _channel = await _connection.CreateChannelAsync(new CreateChannelOptions(
                publisherConfirmationsEnabled: true,
                publisherConfirmationTrackingEnabled: true,
                outstandingPublisherConfirmationsRateLimiter: new ThrottlingRateLimiter(_batchSize * 2)
            ), token)
                .ConfigureAwait(false);

            return _channel;
        }

        private Task ConnectionOnCallbackExceptionAsync(object? sender, CallbackExceptionEventArgs args)
        {
            _logger.SafeLogError(args.Exception, "Callback error");
            return Task.CompletedTask;
        }

        private Task ConnectionOnConnectionShutdownAsync(object? sender, ShutdownEventArgs args)
        {
            _logger.SafeLogInfo($"Disconnected from RabbitMQ ({args.ReplyText})");
            return Task.CompletedTask;
        }

        private Task ConnectionOnConnectionUnblockedAsync(object? sender, AsyncEventArgs args)
        {
            _logger.SafeLogInfo($"Connection unblocked");
            return Task.CompletedTask;
        }

        private Task ConnectionOnConnectionBlockedAsync(object? sender, ConnectionBlockedEventArgs args)
        {
            _logger.SafeLogInfo($"Connection blocked");
            return Task.CompletedTask;
        }

        private IConnection? _connection;
        private IChannel? _channel;

        private readonly RabbitMqPublisherOptions _options;
        private readonly int _batchSize;
        private readonly ILogger<RabbitMqPublisher>? _logger;

        private readonly List<ValueTask> _pendingTasks = new List<ValueTask>();
    }
}