using System.Text;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

using PgOutput2Json.Core;

namespace PgOutput2Json.RabbitMq
{
    public class MessagePublisher: IDisposable
    {
        public MessagePublisher(string[] hostNames,
                                string username,
                                string password,
                                string exchangeName = "pgoutput2json",
                                bool declareExchange = true,
                                string virtualHost = "/",
                                int port = AmqpTcpEndpoint.UseDefaultPort,
                                ILogger<MessagePublisher>? logger = null)
        {
            _connectionFactory = new ConnectionFactory
            {
                HostName = hostNames[0],
                Port = port,
                UserName = username,
                Password = password,
                VirtualHost = virtualHost,
                AutomaticRecoveryEnabled = true,
                TopologyRecoveryEnabled = true,
                RequestedHeartbeat = TimeSpan.FromSeconds(60)
            };

            _hostNames = hostNames;
            _exchangeName = exchangeName;
            _declareExchange = declareExchange;
            _logger = logger;
        }

        public void PublishMessage(string body, string messageType, string routingKey, bool persistent = true)
        {
            EnsureModelExists();

            SafeLogDebug(body);

            _basicProperties!.Type = messageType;
            _basicProperties.Persistent = persistent;

            var bodyBytes = Encoding.UTF8.GetBytes(body);

            _model!.BasicPublish(_exchangeName, routingKey, _basicProperties, bodyBytes);
        }

        public void WaitForConfirmsOrDie(TimeSpan timeout)
        {
            if (_model == null) throw new InvalidOperationException("Model is disposed");
            _model.WaitForConfirmsOrDie(timeout);

            SafeLogInfo("Confirmed RabbitMQ");
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

            SafeLogInfo("Connecting...");

            try
            {
                _connection = _connectionFactory.CreateConnection(_hostNames);
                _connection.CallbackException += ConnectionOnCallbackException;
                _connection.ConnectionBlocked += ConnectionOnConnectionBlocked;
                _connection.ConnectionUnblocked += ConnectionOnConnectionUnblocked;
                _connection.ConnectionShutdown += ConnectionOnConnectionShutdown;
            }
            catch (Exception ex)
            {
                SafeLogError(ex, "Could not connect to RabbitMQ server");
                throw;
            }

            SafeLogInfo("Connected");
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

                if (_declareExchange)
                {
                    _model.ExchangeDeclare(_exchangeName, ExchangeType.Topic, true);
                }

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
            SafeLogInfo($"Disconnected ({args.ReplyText})");
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

        private readonly string[] _hostNames;
        private readonly string _exchangeName;
        private readonly bool _declareExchange;
        private readonly ILogger<MessagePublisher>? _logger;
        private readonly ConnectionFactory _connectionFactory;
    }
}