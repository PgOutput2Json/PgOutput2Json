﻿using System;
using System.Text;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace PgOutput2Json.RabbitMq
{
    public class RabbitMqPublisher: IMessagePublisher
    {
        private int _batchCount = 0;

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

        public bool Publish(string json, string tableName, string keyColumnValue, int partition)
        {
            EnsureModelExists();

            try
            {
                if (_options.HostNames.Length == 0 || !_options.PersistencyConfigurationByTable.TryGetValue(tableName, out var persistent))
                {
                    persistent = _options.UsePersistentMessagesByDefault;
                }

                _basicProperties!.Type = tableName;
                _basicProperties.Persistent = persistent;

                var routingKey = tableName + "." + partition;

                SafeLogDebug($"Publishing to Exchange={_options.ExchangeName}, RoutingKey={routingKey}, Body={json}");

                var body = Encoding.UTF8.GetBytes(json);

                _model!.BasicPublish(_options.ExchangeName, routingKey, _basicProperties, body);

                if (++_batchCount >= 100)
                {
                    ForceConfirm();
                    return true;
                }

                return false;
            }
            catch
            {
                CloseConnection();
                throw;
            }
        }

        public void ForceConfirm()
        {
            EnsureModelExists();

            _model!.WaitForConfirmsOrDie(TimeSpan.FromSeconds(20));
            _batchCount = 0;
        }

        public virtual void Dispose()
        {
            CloseConnection();
        }

        private void CloseConnection()
        {
            _model.TryDispose(_logger);
            _model = null;

            _connection.TryClose(_logger);
            _connection.TryDispose(_logger);
            _connection = null;
        }

        private void EnsureConnectionExists()
        {
            if (_connection != null) return;

            SafeLogInfo("Connecting to RabbitMQ");

            try
            {
                _connection = _connectionFactory.CreateConnection(_options.HostNames);
                _connection.CallbackException += ConnectionOnCallbackException;
                _connection.ConnectionBlocked += ConnectionOnConnectionBlocked;
                _connection.ConnectionUnblocked += ConnectionOnConnectionUnblocked;
                _connection.ConnectionShutdown += ConnectionOnConnectionShutdown;
            }
            catch (Exception ex)
            {
                SafeLogError(ex, "Could not connect to RabbitMQ");
                throw;
            }

            SafeLogInfo("Connected to RabbitMQ");
        }

        private void EnsureModelExists()
        {
            if (_model != null)
            {
                if (_model.IsOpen)
                {
                    return;
                }

                _model.TryDispose(_logger);
                _model = null;
            }

            try
            {
                EnsureConnectionExists();

                _model = _connection!.CreateModel();
                _model.ConfirmSelect(); // enable publisher acknowledgments

                _basicProperties = _model.CreateBasicProperties();
            }
            catch
            {
                _model.TryDispose(_logger);
                _model = null;

                throw;
            }
        }

        private void SafeLogDebug(string message)
        {
            try
            {
                if (_logger != null && _logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.LogDebug(message);
                }
            }
            catch
            {
            }
        }

        private void SafeLogInfo(string message)
        {
            try
            {
                if (_logger != null && _logger.IsEnabled(LogLevel.Information))
                {
                    _logger.LogInformation(message);
                }
            }
            catch
            {
            }
        }

        private void SafeLogError(Exception ex, string message)
        {
            try
            {
                if (_logger != null && _logger.IsEnabled(LogLevel.Error))
                {
                    _logger.LogError(ex, message);
                }
            }
            catch
            {
            }
        }

        private void ConnectionOnCallbackException(object? sender, CallbackExceptionEventArgs args)
        {
            SafeLogError(args.Exception, "Callback error");
        }

        private void ConnectionOnConnectionShutdown(object? sender, ShutdownEventArgs args)
        {
            SafeLogInfo($"Disconnected from RabbitMQ ({args.ReplyText})");
        }

        private void ConnectionOnConnectionUnblocked(object? sender, EventArgs args)
        {
            SafeLogInfo($"Connection unblocked");
        }

        private void ConnectionOnConnectionBlocked(object? sender, ConnectionBlockedEventArgs args)
        {
            SafeLogInfo($"Connection blocked");
        }

        private IConnection? _connection;
        private IModel? _model;
        private IBasicProperties? _basicProperties;

        private readonly RabbitMqOptions _options;
        private readonly ILogger<RabbitMqPublisher>? _logger;
        private readonly ConnectionFactory _connectionFactory;
    }
}