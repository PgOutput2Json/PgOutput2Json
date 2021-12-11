using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;

namespace PgOutput2Json
{
    internal sealed class ReplicationListener: IDisposable
    {
        private readonly ILogger<ReplicationListener>? _logger;

        private readonly ReplicationListenerOptions _options;
        private readonly JsonOptions _jsonOptions;
        private readonly StringBuilder _jsonBuilder = new StringBuilder(256);
        private readonly StringBuilder _tableNameBuilder = new StringBuilder(256);
        private readonly StringBuilder _keyColValueBuilder = new StringBuilder(256);

        private readonly object _lock = new object();

        private readonly Timer _confirmTimer;
        private readonly TimeSpan _confirmTimerPeriod;
        private volatile bool _confirmTimerRunning;

        private ManualResetEvent _stoppedEvent = new ManualResetEvent(true);
        private bool _disposed;

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
                                   JsonOptions jsonOptions,
                                   ILogger<ReplicationListener>? logger = null)
            : this(options, jsonOptions, TimeSpan.FromSeconds(5), logger)
        { 
        }

        public ReplicationListener(ReplicationListenerOptions options,
                                   JsonOptions jsonOptions,
                                   TimeSpan confirmTimerPeriod,
                                   ILogger<ReplicationListener>? logger = null)
        {
            _options = options;
            _jsonOptions = jsonOptions;
            _confirmTimerPeriod = confirmTimerPeriod;
            _logger = logger;
            _confirmTimer = new Timer(ConfirmCallback);
        }

        public void Dispose()
        {
            lock (_lock)
            {
                if (_disposed) return;
                _disposed = true;

                _cancellationTokenSource.TryCancel(_logger);
            }

            if (!_stoppedEvent.WaitOne(5000))
            {
                SafeLogWarn("Timed out waiting for the listener to stop");
            }

            _confirmTimer.TryDispose(_logger);
        }

        public async Task ListenForChanges(CancellationToken cancellationToken)
        {
            lock (_lock)
            {
                if (_disposed) throw new ObjectDisposedException(nameof(ReplicationListener));
            }

            _stoppedEvent.Reset();

            while (true)
            {
                try
                {
                    if (cancellationToken.IsCancellationRequested) break;

                    _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

                    _connection = new LogicalReplicationConnection(_options.ConnectionString);
                    _connection.WalReceiverStatusInterval = TimeSpan.FromSeconds(5);

                    await _connection.Open();

                    SafeLogInfo("Connected to PostgreSQL");

                    PgOutputReplicationSlot slot;

                    if (_options.ReplicationSlotName != string.Empty)
                    {
                        slot = new PgOutputReplicationSlot(_options.ReplicationSlotName);
                    }
                    else
                    {
                        var slotName = $"pg2j_{Guid.NewGuid().ToString().Replace("-", "")}";
                        slot = await _connection.CreatePgOutputReplicationSlot(slotName, true, 
                            cancellationToken: _cancellationTokenSource.Token);
                    }

                    var replicationOptions = new PgOutputReplicationOptions(_options.PublicationNames, 1);

                    lock (_lock)
                    {
                        if (_disposed) break;

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
                                             _jsonOptions.WriteNulls);
                        }
                        else if (message is UpdateMessage updateMsg)
                        {
                            partition = await WriteTuple(updateMsg.NewRow,
                                             updateMsg.Relation,
                                             "U",
                                             commitTimeStamp,
                                             updateMsg.ServerClock,
                                             _jsonOptions.WriteNulls);
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

                        lock (_lock)
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
                        SafeLogError(_confirmHandlerError, $"Error in {nameof(ConfirmCallback)}. Waiting for 10 seconds...");
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

                await Delay(10000, cancellationToken);
            }

            StopTimerAndDisposeResources();

            _stoppedEvent.Set();

            SafeLogInfo("Disconnected from PostgreSQL");
        }

        private async Task Delay(int time, CancellationToken cancellationToken)
        {
            try
            {
                await Task.Delay(time, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                // ignore if task was cancelled 
            }
            catch (Exception ex)
            {
                SafeLogError(ex, "Error while waiting to reconnect");
            }
        }

        private void StopTimerAndDisposeResources()
        {
            lock (_lock)
            {
                if (!_disposed)
                {
                    _confirmTimer.Change(Timeout.Infinite, Timeout.Infinite);
                }

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
            lock (_lock)
            {
                if (_disposed || !_confirmTimerRunning) return;

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
                        _cancellationTokenSource.TryCancel(_logger);
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
            
            var tableName = _tableNameBuilder.ToString();

            _jsonBuilder.Clear();
            _jsonBuilder.Append("{\"_ct\":\"");
            _jsonBuilder.Append(changeType);
            _jsonBuilder.Append('"');

            if (_jsonOptions.WriteTimestamps)
            {
                _jsonBuilder.Append(",");
                _jsonBuilder.Append("\"_cts\":\"");
                _jsonBuilder.Append(commitTimeStamp.Ticks);
                _jsonBuilder.Append("\",");
                _jsonBuilder.Append("\"_mts\":\"");
                _jsonBuilder.Append(messageTimeStamp.Ticks);
                _jsonBuilder.Append('"');
            }

            if (_jsonOptions.WriteTableNames)
            {
                _jsonBuilder.Append(",");
                _jsonBuilder.Append("\"_tbl\":\"");
                JsonUtils.EscapeJson(_jsonBuilder, relation.Namespace);
                _jsonBuilder.Append('.');
                JsonUtils.EscapeJson(_jsonBuilder, relation.RelationName);
                _jsonBuilder.Append('"');
            }

            _keyColValueBuilder.Clear();

            int finalHash = 0x12345678;

            if (!_options.KeyColumns.TryGetValue(tableName, out var keyColumn))
            {
                keyColumn = null;
            }

            if (!_options.IncludedColumns.TryGetValue(tableName, out var includedCols))
            {
                includedCols = null;
            }

            var i = 0;
            await foreach (var value in tuple)
            {
                var col = relation.Columns[i++];

                if (value.IsDBNull && !sendNulls) continue;

                if (value.IsDBNull || value.Kind == TupleDataKind.TextValue)
                {
                    var isKeyColumn = false;
                    if (keyColumn != null)
                    {
                        foreach (var c in keyColumn.ColumnNames)
                        {
                            if (c == col.ColumnName)
                            {
                                isKeyColumn = true;
                                break;
                            }
                        }
                    }

                    var isIncluded = includedCols == null; // if not specified, included by default
                    if (includedCols != null)
                    {
                        foreach (var c in includedCols)
                        {
                            if (c == col.ColumnName)
                            {
                                isIncluded = true;
                                break;
                            }
                        }
                    }

                    if (!isIncluded && !isKeyColumn)
                    {
                        if (!value.IsDBNull)
                        {
                            // this is a hack to skip bytes (dispose does the trick)
                            // otherwise, npgsql throws exception
                            await value.Get<string>(_cancellationTokenSource!.Token);
                        }
                        continue;
                    }

                    StringBuilder? valueBuilder = null;

                    if (isKeyColumn)
                    {
                        valueBuilder = _keyColValueBuilder;
                        if (valueBuilder.Length > 0)
                        {
                            valueBuilder.Append('|');
                        }
                    }

                    _jsonBuilder.Append(",\"");
                    JsonUtils.EscapeJson(_jsonBuilder, col.ColumnName);
                    _jsonBuilder.Append("\":");

                    if (value.IsDBNull)
                    {
                        _jsonBuilder.Append("null");
                    }
                    else if (value.Kind == TupleDataKind.TextValue)
                    {
                        var type = value.GetPostgresType();
                        var pgOid = (PgOid)type.OID;

                        // we know _cancellationTokenSource is not null here since it gets disposed on end of "start" loop
                        var colValue = await value.Get<string>(_cancellationTokenSource!.Token);
                        valueBuilder?.Append(colValue);

                        int hash;

                        if (pgOid.IsNumber())
                        {
                            hash = JsonUtils.WriteNumber(_jsonBuilder, colValue);
                        }
                        else if (pgOid.IsBoolean())
                        {
                            hash = JsonUtils.WriteBoolean(_jsonBuilder, colValue);
                        }
                        else if (pgOid.IsByte())
                        {
                            hash = JsonUtils.WriteByte(_jsonBuilder, colValue);
                        }
                        else
                        {
                            hash = JsonUtils.WriteText(_jsonBuilder, colValue);
                        }

                        if (isKeyColumn) finalHash ^= hash;
                    }
                }
            }
                            
            _jsonBuilder.Append('}');

            var partition = keyColumn != null ? finalHash % keyColumn.PartitionCount : 0;

            if (_options.PartitionFilter != null 
                && (partition < _options.PartitionFilter.FromInclusive || partition >= _options.PartitionFilter.ToExclusive))
            {
                partition = -1; // this will prevent event from firing
            }

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
