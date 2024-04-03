using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;
using NpgsqlTypes;

namespace PgOutput2Json
{
    internal sealed class ReplicationListener
    {
        private readonly ILoggerFactory? _loggerFactory;
        private readonly ILogger<ReplicationListener>? _logger;

        private readonly ReplicationListenerOptions _options;
        private readonly JsonOptions _jsonOptions;
        private readonly StringBuilder _jsonBuilder = new StringBuilder(256);
        private readonly StringBuilder _tableNameBuilder = new StringBuilder(256);
        private readonly StringBuilder _keyColValueBuilder = new StringBuilder(256);

        private readonly IMessagePublisherFactory _messagePublisherFactory;

        public ReplicationListener(IMessagePublisherFactory messagePublisherFactory,
                                   ReplicationListenerOptions options,
                                   JsonOptions jsonOptions,
                                   ILoggerFactory? loggerFactory)
        {
            _messagePublisherFactory = messagePublisherFactory;
            _options = options;
            _jsonOptions = jsonOptions;

            _loggerFactory = loggerFactory;
            _logger = loggerFactory?.CreateLogger<ReplicationListener>();
        }

        public async Task ListenForChanges(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var messagePublisher = _messagePublisherFactory.CreateMessagePublisher(_loggerFactory);
                try
                {
                    await using var connection = new LogicalReplicationConnection(_options.ConnectionString);

                    connection.WalReceiverStatusInterval = TimeSpan.FromSeconds(5);

                    await connection.Open(cancellationToken);

                    SafeLogInfo("Connected to PostgreSQL");

                    PgOutputReplicationSlot slot;

                    if (_options.ReplicationSlotName != string.Empty)
                    {
                        slot = new PgOutputReplicationSlot(_options.ReplicationSlotName);
                    }
                    else
                    {
                        var slotName = $"pg2j_{Guid.NewGuid().ToString().Replace("-", "")}";
                        slot = await connection.CreatePgOutputReplicationSlot(slotName, true, cancellationToken: cancellationToken);
                    }

                    var replicationOptions = new PgOutputReplicationOptions(_options.PublicationNames, 1);

                    DateTime commitTimeStamp = DateTime.UtcNow;

                    NpgsqlLogSequenceNumber? lastWalEnd = null;

                    using var forceConfirmTimer = new Timer((_) =>
                    {
                        if (!lastWalEnd.HasValue) return;
                        
                        lock (messagePublisher)
                        {
                            messagePublisher.ForceConfirm();

                            connection.SetReplicationStatus(lastWalEnd.Value);
                            lastWalEnd = null;
                        }
                    });

                    await foreach (var message in connection.StartReplication(slot, replicationOptions, cancellationToken))
                    {
                        lastWalEnd = message.WalEnd;

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
                                             _jsonOptions.WriteNulls,
                                             cancellationToken);
                        }
                        else if (message is UpdateMessage updateMsg)
                        {
                            partition = await WriteTuple(updateMsg.NewRow,
                                             updateMsg.Relation,
                                             "U",
                                             commitTimeStamp,
                                             updateMsg.ServerClock,
                                             _jsonOptions.WriteNulls,
                                             cancellationToken);
                        }
                        else if (message is KeyDeleteMessage keyDeleteMsg)
                        {
                            partition = await WriteTuple(keyDeleteMsg.Key,
                                             keyDeleteMsg.Relation,
                                             "D",
                                             commitTimeStamp,
                                             keyDeleteMsg.ServerClock,
                                             false,
                                             cancellationToken);
                        }
                        else if (message is FullDeleteMessage fullDeleteMsg)
                        {
                            partition = await WriteTuple(fullDeleteMsg.OldRow,
                                             fullDeleteMsg.Relation,
                                             "D",
                                             commitTimeStamp,
                                             fullDeleteMsg.ServerClock,
                                             false,
                                             cancellationToken);
                        }

                        if (partition >= 0)
                        {
                            lock (messagePublisher)
                            {
                                var confirm = messagePublisher
                                    .Publish(_jsonBuilder.ToString(), _tableNameBuilder.ToString(), _keyColValueBuilder.ToString(), partition);

                                if (confirm)
                                {
                                    forceConfirmTimer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);

                                    connection.SetReplicationStatus(message.WalEnd);
                                    lastWalEnd = null;

                                    SafeLogInfo("Confirmed PostgreSQL");   
                                }
                                else
                                {
                                    forceConfirmTimer.Change(TimeSpan.FromSeconds(10), Timeout.InfiniteTimeSpan);
                                }
                            }
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
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
                finally
                {
                    messagePublisher.TryDispose(_logger);
                }

                await Delay(10000, cancellationToken);
            }

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

        private async Task<int> WriteTuple(ReplicationTuple tuple,
                                           RelationMessage relation,
                                           string changeType,
                                           DateTime commitTimeStamp,
                                           DateTime messageTimeStamp,
                                           bool sendNulls,
                                           CancellationToken cancellationToken)
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
                JsonUtils.EscapeText(_jsonBuilder, relation.Namespace);
                _jsonBuilder.Append('.');
                JsonUtils.EscapeText(_jsonBuilder, relation.RelationName);
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
                            await value.Get<string>(cancellationToken);
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
                    JsonUtils.EscapeText(_jsonBuilder, col.ColumnName);
                    _jsonBuilder.Append("\":");

                    if (value.IsDBNull)
                    {
                        _jsonBuilder.Append("null");
                    }
                    else if (value.Kind == TupleDataKind.TextValue)
                    {
                        var pgOid = (PgOid)col.DataTypeId;

                        var colValue = await value.Get<string>(cancellationToken);
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
                        else if (pgOid.IsArrayOfNumber())
                        {
                            hash = JsonUtils.WriteArrayOfNumber(_jsonBuilder, colValue);
                        }
                        else if (pgOid.IsArrayOfByte())
                        {
                            hash = JsonUtils.WriteArrayOfByte(_jsonBuilder, colValue);
                        }
                        else if (pgOid.IsArrayOfBoolean())
                        {
                            hash = JsonUtils.WriteArrayOfBoolean(_jsonBuilder, colValue);
                        }
                        else if (pgOid.IsArrayOfText())
                        {
                            hash = JsonUtils.WriteArrayOfText(_jsonBuilder, colValue);
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
