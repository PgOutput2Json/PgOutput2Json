using RabbitMQ.Client;
using RabbitMQ.Client.Events;

using PgOutput2Json.Core;

namespace PgOutput2Json.RabbitMq
{
    public class MessagePublisher
    {
        public MessagePublisher(string[] hostNames,
                                string username,
                                string password,
                                string exchangeName = "pgoutput2json",
                                bool declareExchange = true,
                                string virtualHost = "/",
                                int port = AmqpTcpEndpoint.UseDefaultPort,
                                LoggingErrorHandler? errorHandler = null,
                                LoggingHandler? infoHandler = null)
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
            _errorHandler = errorHandler;
            _infoHandler = infoHandler;
        }

        public void PublishMessage(ReadOnlyMemory<byte> body, string messageType, string routingKey, bool persistent = true)
        {
            EnsureModelExists();

            _basicProperties!.Type = messageType;
            _basicProperties.Persistent = persistent;

            _model!.BasicPublish(_exchangeName, routingKey, _basicProperties, body);
        }

        public void WaitForConfirmsOrDie(TimeSpan timeout)
        {
            if (_model == null) throw new InvalidOperationException("Model is disposed");
            _model.WaitForConfirmsOrDie(timeout);
        }

        public virtual void Dispose()
        {
            CloseConnection();
        }

        private void CloseConnection()
        {
            _model.TryDispose(_errorHandler);
            _model = null;

            _connection.TryClose(_errorHandler);
            _connection.TryDispose(_errorHandler);
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
                SafeErrorLog(ex, "Could not connect to RabbitMQ server");
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

                _model.TryDispose(_errorHandler);
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
                _model.TryDispose(_errorHandler);
                _model = null;

                throw;
            }
        }

        private void SafeLogInfo(string message)
        {
            try
            {
                _infoHandler?.Invoke(message);
            }
            catch
            {
            }
        }

        private void SafeErrorLog(Exception ex, string message)
        {
            try
            {
                _errorHandler?.Invoke(ex, message);
            }
            catch
            {
            }
        }

        private void ConnectionOnCallbackException(object? sender, CallbackExceptionEventArgs args)
        {
            SafeErrorLog(args.Exception, "Callback error");
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
        private readonly ConnectionFactory _connectionFactory;
        private readonly LoggingErrorHandler? _errorHandler;
        private readonly LoggingHandler? _infoHandler;
    }
}