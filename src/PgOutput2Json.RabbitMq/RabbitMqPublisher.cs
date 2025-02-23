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
    public class RabbitMqPublisher: IMessagePublisher
    {
        public RabbitMqPublisher(RabbitMqOptions options, ILogger<RabbitMqPublisher>? logger = null)
        {
            _connectionFactory = new ConnectionFactory
            {
                HostName = options.HostNames[0],
                Port = options.Port,
                UserName = options.Username,
                Password = options.Password,
                VirtualHost = options.VirtualHost,
                AutomaticRecoveryEnabled = true,
                TopologyRecoveryEnabled = true,
                RequestedHeartbeat = TimeSpan.FromSeconds(60)
            };

            _options = options;
            _logger = logger;
        }

        public async Task<bool> PublishAsync(string json, string tableName, string keyColumnValue, int partition, CancellationToken token)
        {
            var channel = await ConnectAsync(token)
                .ConfigureAwait(false);

            if (_options.HostNames.Length == 0 || !_options.PersistencyConfigurationByTable.TryGetValue(tableName, out var persistent))
            {
                persistent = _options.UsePersistentMessagesByDefault;
            }

            var routingKey = tableName + "." + partition;

            _logger.SafeLogDebug($"Publishing to Exchange={_options.ExchangeName}, RoutingKey={routingKey}, Body={json}");

            var body = Encoding.UTF8.GetBytes(json);

            var basicProperties = new BasicProperties { Type = tableName, Persistent = persistent };

            var task = channel.BasicPublishAsync(_options.ExchangeName, routingKey, false, basicProperties, body, token);

            _pendingTasks.Add(task);

            return await MaybeAwaitPublishes()
                .ConfigureAwait(false);
        }

        public async Task ForceConfirmAsync(CancellationToken token)
        {
            await MaybeAwaitPublishes(force: true)
                .ConfigureAwait(false);
        }

        public virtual async ValueTask DisposeAsync()
        {
            _pendingTasks.Clear();

            _channel.TryDispose(_logger);
            _channel = null;

            await _connection.TryCloseAsync(_logger)
                .ConfigureAwait(false);

            _connection.TryDispose(_logger);
            _connection = null;
        }

        private async ValueTask<IChannel> ConnectAsync(CancellationToken token)
        {
            if (_channel != null) return _channel;

            _logger.SafeLogInfo("Connecting to RabbitMQ");

            _connection = await _connectionFactory.CreateConnectionAsync(_options.HostNames, token)
                .ConfigureAwait(false);

            _connection.CallbackExceptionAsync += ConnectionOnCallbackException;
            _connection.ConnectionBlockedAsync += ConnectionOnConnectionBlocked;
            _connection.ConnectionUnblockedAsync += ConnectionOnConnectionUnblocked;
            _connection.ConnectionShutdownAsync += ConnectionOnConnectionShutdown;

            _logger.SafeLogInfo("Connected to RabbitMQ");

            _channel = await _connection!.CreateChannelAsync(new CreateChannelOptions(
                publisherConfirmationsEnabled: true,
                publisherConfirmationTrackingEnabled: true,
                outstandingPublisherConfirmationsRateLimiter: new ThrottlingRateLimiter(_options.BatchSize * 2)
            ))
                .ConfigureAwait(false);

            return _channel;
        }

        private async ValueTask<bool> MaybeAwaitPublishes(bool force = false)
        {
            if (!force && _pendingTasks.Count < _options.BatchSize)
            {
                return false;
            }

            foreach (var pt in _pendingTasks)
            {
                await pt.ConfigureAwait(false);
            }

            _pendingTasks.Clear();
            return true;
        }

        private Task ConnectionOnCallbackException(object? sender, CallbackExceptionEventArgs args)
        {
            _logger.SafeLogError(args.Exception, "Callback error");
            return Task.CompletedTask;
        }

        private Task ConnectionOnConnectionShutdown(object? sender, ShutdownEventArgs args)
        {
            _logger.SafeLogInfo($"Disconnected from RabbitMQ ({args.ReplyText})");
            return Task.CompletedTask;
        }

        private Task ConnectionOnConnectionUnblocked(object? sender, AsyncEventArgs args)
        {
            _logger.SafeLogInfo($"Connection unblocked");
            return Task.CompletedTask;
        }

        private Task ConnectionOnConnectionBlocked(object? sender, ConnectionBlockedEventArgs args)
        {
            _logger.SafeLogInfo($"Connection blocked");
            return Task.CompletedTask;
        }

        private IConnection? _connection;
        private IChannel? _channel;

        private readonly RabbitMqOptions _options;
        private readonly ILogger<RabbitMqPublisher>? _logger;
        private readonly ConnectionFactory _connectionFactory;

        private readonly List<ValueTask> _pendingTasks = new List<ValueTask>();
    }
}