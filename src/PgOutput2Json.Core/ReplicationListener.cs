using Microsoft.Extensions.Logging;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;
using System.Text;

namespace PgOutput2Json.Core
{
    internal sealed class ReplicationListener: IDisposable
    {
        private readonly ILogger<ReplicationListener>? _logger;

        private readonly ReplicationListenerOptions _options;
        private readonly StringBuilder _jsonBuilder = new StringBuilder(256);
        private readonly StringBuilder _tableNameBuilder = new StringBuilder(256);
        private readonly StringBuilder _keyColValueBuilder = new StringBuilder(256);

        private readonly Timer _confirmTimer;
        private readonly TimeSpan _confirmTimerPeriod;
        private readonly object _confirmTimerLock = new object();
        private volatile bool _confirmTimerRunning;
        private ulong _walEnd;

        private LogicalReplicationConnection? _connection;
        private CancellationTokenSource? _cancellationTokenSource;
            
        private Exception? _confirmHandlerError;

        /// <summary>
        /// Called on every change of a database row. 
        /// </summary>
        public event MessageHandler? MessageHandler;

        /// <summary>
        /// Called periodically (by default every 5 sec) if there are messsages to be confirmed.
        /// </summary>
        public event ConfirmHandler? ConfirmHandler;

        public ReplicationListener(ReplicationListenerOptions options,
                                   ILogger<ReplicationListener>? logger = null)
            : this(options, TimeSpan.FromSeconds(5), logger)
        { 
        }

        public ReplicationListener(ReplicationListenerOptions options,
                                   TimeSpan confirmTimerPeriod,
                                   ILogger<ReplicationListener>? logger = null)
        {
            _options = options;
            _confirmTimerPeriod = confirmTimerPeriod;
            _logger = logger;
            _confirmTimer = new Timer(ConfirmCallback);
        }

        public void Dispose()
        {
            _confirmTimer.TryDispose(_logger);
        }

        public async Task ListenForChanges(CancellationToken cancellationToken)
        {
            while (true)
            {
                try
                {
                    _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

                    _connection = new LogicalReplicationConnection(_options.ConnectionString);
                    _connection.WalReceiverStatusInterval = TimeSpan.FromSeconds(5);

                    await _connection.Open();

                    SafeLogInfo("Connected to PostgreSQL");

                    var slot = new PgOutputReplicationSlot(_options.ReplicationSlotName);
                    var replicationOptions = new PgOutputReplicationOptions(_options.PublicationName, 1);

                    lock (_confirmTimer)
                    {
                        _confirmTimer.Change(_confirmTimerPeriod, Timeout.InfiniteTimeSpan);
                        _confirmTimerRunning = true;
                    }

                    DateTime commitTimeStamp = DateTime.UtcNow;

                    await foreach (var message in _connection.StartReplication(slot, replicationOptions, _cancellationTokenSource.Token))
                    {
                        var partition = -1;

                        if (message is BeginMessage beginMsg)
                        {
                            commitTimeStamp = beginMsg.TransactionCommitTimestamp;
                        }
                        else if (message is InsertMessage insertMsg)
                        {
                            partition = await WriteTuple(insertMsg.NewRow,
                                             insertMsg.Relation,
                                             "I",
                                             commitTimeStamp,
                                             insertMsg.ServerClock,
                                             _options.WriteNulls);
                        }
                        else if (message is UpdateMessage updateMsg)
                        {
                            partition = await WriteTuple(updateMsg.NewRow,
                                             updateMsg.Relation,
                                             "U",
                                             commitTimeStamp,
                                             updateMsg.ServerClock,
                                             _options.WriteNulls);
                        }
                        else if (message is KeyDeleteMessage keyDeleteMsg)
                        {
                            partition = await WriteTuple(keyDeleteMsg.Key,
                                             keyDeleteMsg.Relation,
                                             "D",
                                             commitTimeStamp,
                                             keyDeleteMsg.ServerClock,
                                             false);
                        }
                        else if (message is FullDeleteMessage fullDeleteMsg)
                        {
                            partition = await WriteTuple(fullDeleteMsg.OldRow,
                                             fullDeleteMsg.Relation,
                                             "D",
                                             commitTimeStamp,
                                             fullDeleteMsg.ServerClock,
                                             false);
                        }

                        lock (_confirmTimerLock)
                        {
                            _walEnd = (ulong)message.WalEnd;

                            if (partition >= 0)
                            {
                                var confirm = false;
                                MessageHandler?.Invoke(_jsonBuilder.ToString(),
                                                       _tableNameBuilder.ToString(),
                                                       _keyColValueBuilder.ToString(),
                                                       partition,
                                                       ref confirm);

                                if (confirm)
                                {
                                    _connection.SetReplicationStatus(message.WalEnd);
                                    _walEnd = 0;

                                    SafeLogInfo("Confirmed PostgreSQL");
                                }
                            }
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    if (_confirmHandlerError != null)
                    {
                        SafeLogError(_confirmHandlerError, "Error in IdleCallback. Waiting for 10 seconds...");
                    }
                    else
                    {
                        break;
                    }
                }
                catch (Exception ex)
                {
                    if (ex.Message.StartsWith("55006:"))
                    {
                        SafeLogWarn("Slot taken - waiting for 10 seconds...");
                    }
                    else
                    {
                        SafeLogError(ex, "Error in replication listener. Waiting for 10 seconds...");
                    }
                }

                StopTimerAndDisposeResources();

                Thread.Sleep(10000);
            }

            StopTimerAndDisposeResources();
        }

        private void StopTimerAndDisposeResources()
        {
            lock (_confirmTimerLock)
            {
                _confirmTimer.Change(Timeout.Infinite, Timeout.Infinite);

                _confirmTimerRunning = false;
                _walEnd = 0;
                _confirmHandlerError = null;

                _connection.TryDisposeAsync(_logger);
                _connection = null;

                _cancellationTokenSource.TryDispose(_logger);
                _cancellationTokenSource = null;
            }
        }

        private void ConfirmCallback(object? state)
        {
            lock (_confirmTimerLock)
            {
                if (!_confirmTimerRunning) return;

                if (_walEnd > 0)
                {
                    try
                    {
                        ConfirmHandler?.Invoke();
                        _connection?.SetReplicationStatus(new NpgsqlTypes.NpgsqlLogSequenceNumber(_walEnd));
                        _walEnd = 0;

                        SafeLogInfo("Confirmed PostgreSQL");
                    }
                    catch (Exception ex)
                    {
                        _confirmHandlerError = ex;
                        try 
                        {
                            _cancellationTokenSource?.Cancel();
                        }
                        catch (Exception cancelEx)
                        {
                            SafeLogError(cancelEx, "Error cancelling the listener");
                        }
                    }
                }

                _confirmTimer.Change(_confirmTimerPeriod, Timeout.InfiniteTimeSpan);
            }
        }

        private async Task<int> WriteTuple(ReplicationTuple tuple,
                                      RelationMessage relation,
                                      string changeType,
                                      DateTime commitTimeStamp,
                                      DateTime messageTimeStamp,
                                      bool sendNulls)
        {
            _tableNameBuilder.Clear();
            _tableNameBuilder.Append(relation.Namespace);
            _tableNameBuilder.Append('.');
            _tableNameBuilder.Append(relation.RelationName);

            _jsonBuilder.Clear();
            _jsonBuilder.Append("{\"_ct\":\"");
            _jsonBuilder.Append(changeType);
            _jsonBuilder.Append("\",");
            _jsonBuilder.Append("\"_cts\":\"");
            _jsonBuilder.Append(commitTimeStamp.Ticks);
            _jsonBuilder.Append("\",");
            _jsonBuilder.Append("\"_mts\":\"");
            _jsonBuilder.Append(messageTimeStamp.Ticks);
            _jsonBuilder.Append("\",");
            _jsonBuilder.Append("\"_re\":\"");
            _jsonBuilder.Append(relation.Namespace);
            _jsonBuilder.Append('.');
            _jsonBuilder.Append(relation.RelationName);
            _jsonBuilder.Append('"');

            _keyColValueBuilder.Clear();

            int finalHash = 0x12345678;

            if (!_options.KeyColumns.TryGetValue(_tableNameBuilder.ToString(), out var keyColumn))
            {
                keyColumn = null;
            }

            var i = 0;
            await foreach (var value in tuple)
            {
                var col = relation.Columns[i++];

                if (value.IsDBNull && !sendNulls) continue;

                if (value.IsDBNull || value.Kind == TupleDataKind.TextValue)
                {
                    StringBuilder? valueBuilder = null;

                    var isKeyColumn = keyColumn?.ColumnNames.Contains(col.ColumnName) ?? false;

                    if (isKeyColumn)
                    {
                        valueBuilder = _keyColValueBuilder;
                        if (valueBuilder.Length > 0)
                        {
                            // preparations for multiple key column support in the future
                            valueBuilder.Append('|');
                        }
                    }

                    _jsonBuilder.Append(",\"");
                    _jsonBuilder.Append(col.ColumnName);
                    _jsonBuilder.Append("\":");

                    if (value.IsDBNull)
                    {
                        _jsonBuilder.Append("null");
                    }
                    else if (value.Kind == TupleDataKind.TextValue)
                    {
                        var type = value.GetPostgresType();
                        var pgOid = (PgOid)type.OID;

                        int hash;

                        if (pgOid.IsNumber())
                        {
                            hash = JsonUtils.WriteNumber(_jsonBuilder, valueBuilder, value.GetTextReader());
                        }
                        else if (pgOid.IsBoolean())
                        {
                            hash = JsonUtils.WriteBoolean(_jsonBuilder, valueBuilder, value.GetTextReader());
                        }
                        else if (pgOid.IsByte())
                        {
                            hash = JsonUtils.WriteByte(_jsonBuilder, valueBuilder, value.GetTextReader());
                        }
                        else
                        {
                            hash = JsonUtils.WriteText(_jsonBuilder, valueBuilder, value.GetTextReader());
                        }

                        if (isKeyColumn) finalHash ^= hash;
                    }
                }
            }
                            
            _jsonBuilder.Append('}');

            var partition = keyColumn != null ? finalHash % keyColumn.PartitionCount : 0;
            return partition;
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

        private void SafeLogWarn(string message)
        {
            try
            {
                if (_logger != null && _logger.IsEnabled(LogLevel.Warning))
                {
                    _logger.LogWarning(message);
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
    }
}
