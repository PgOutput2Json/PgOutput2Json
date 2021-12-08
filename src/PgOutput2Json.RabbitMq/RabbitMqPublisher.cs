using System.Text;
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

            _hostNames = options.HostNames;
            _exchangeName = options.ExchangeName;
            _logger = logger;
        }

        public void Publish(string json, string tableName, string keyColumnValue, int partition)
        {
            EnsureModelExists();

            SafeLogDebug(json);

            _basicProperties!.Type = tableName;
            _basicProperties.Persistent = true;

            var body = Encoding.UTF8.GetBytes(json);

            _model!.BasicPublish(_exchangeName, tableName + "." + partition, _basicProperties, body);
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

            SafeLogInfo("Connecting to RabbitMQ");

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

        private readonly string[] _hostNames;
        private readonly string _exchangeName;
        private readonly ILogger<RabbitMqPublisher>? _logger;
        private readonly ConnectionFactory _connectionFactory;
    }
}