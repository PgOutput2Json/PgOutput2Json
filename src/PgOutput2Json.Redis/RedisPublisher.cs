using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

using StackExchange.Redis;

namespace PgOutput2Json.Redis
{
    public class RedisPublisher : MessagePublisher
    {
        private const string ConfigKeySuffix = "__pg2j_config";

        private const string WalEndField = "wal_end";
        private const string MessageNoField = "message_no";

        public RedisPublisher(RedisPublisherOptions options, ILogger<RedisPublisher>? logger = null, bool useDeduplication = true)
        {
            _options = options;
            _logger = logger;
            _useDeduplication = useDeduplication;
        }

        public override async Task PublishAsync(JsonMessage msg, CancellationToken token)
        {
            _redis ??= await ConnectionMultiplexer.ConnectAsync(_options.Redis)
                .ConfigureAwait(false);

            var json = msg.Json.ToString();
            var tableName = msg.TableName.ToString();
            var partition = GetPartitionId(msg, tableName);

            string name;

            if (_options.StreamNameSuffix == StreamNameSuffix.TableName)
            {
                name = string.Join(':', _options.StreamName, tableName);
            }
            else if (_options.StreamNameSuffix == StreamNameSuffix.TableNameAndPartition)
            {
                name = string.Join(':', _options.StreamName, tableName, partition);
            }
            else
            {
                name = _options.StreamName;
            }

            if (_options.PublishMode == PublishMode.Channel)
            {
                var task = _redis.GetSubscriber().PublishAsync(RedisChannel.Literal(name), json, CommandFlags.DemandMaster);

                _publishedTasks.Add(task);

                if (_logger != null && _logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.LogDebug("Published to Channel={ChannelName}, Body={Body}", name, json);
                }
            }
            else
            {
                var task = _redis.GetDatabase().StreamAddAsync(name, "m", json, flags: CommandFlags.DemandMaster);

                _publishedTasks.Add(task);

                if (_logger != null && _logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.LogDebug("Published to Stream={StreamName}, Body={Body}", name, json);
                }
            }

            _lastWal = msg.TxFinalLsn;
            _lastMessageNo = msg.MessageNo;
        }

        private int GetPartitionId(JsonMessage msg, string tableName)
        {
            if (!_options.TablePartitionCounts.TryGetValue(tableName, out var partitionCount) || partitionCount <= 0)
            {
                return 0;
            }

            var partitionKey = msg.PartitionKolValue.ToString();
            if (partitionKey == string.Empty)
            {
                partitionKey = msg.KeyKolValue.ToString();
            }

            // murmur2, the same client-side routing the Kafka adapter uses - stable across
            // restarts, unlike string.GetHashCode(), and without the Math.Abs(int.MinValue) overflow
            return partitionKey != string.Empty ? (MurmurHash2.Hash(partitionKey) & 0x7fffffff) % partitionCount : 0;
        }

        public override async Task ConfirmAsync(CancellationToken token)
        {
            foreach (var pt in _publishedTasks)
            {
                await pt.ConfigureAwait(false);
            }

            DisposeTasks();

            if (_redis == null || !_useDeduplication) return;

            // data exporter messages have no LSN info (0,0) - they must not overwrite the replication position;
            // channels carry no history, so there is nothing to deduplicate against
            if (_options.PublishMode == PublishMode.Channel || (_lastWal == 0 && _lastMessageNo == 0)) return;

            // both fields land in one atomic HSET - the watermark cannot be caught half-written
            await _redis.GetDatabase().HashSetAsync(ConfigKeyName, [
                new HashEntry(WalEndField, _lastWal.ToString(CultureInfo.InvariantCulture)),
                new HashEntry(MessageNoField, _lastMessageNo.ToString(CultureInfo.InvariantCulture))
            ], flags: CommandFlags.DemandMaster).ConfigureAwait(false);
        }

        public override async Task<(ulong, ulong)> GetLastPublishedWalSeqAsync(CancellationToken token)
        {
            // without deduplication there is no need to read the last published position
            if (!_useDeduplication) return (0UL, 0UL);

            if (_options.PublishMode == PublishMode.Channel) return (0, 0); // cannot do de-duplication with channels

            _redis ??= await ConnectionMultiplexer.ConnectAsync(_options.Redis)
                .ConfigureAwait(false);

            var values = await _redis.GetDatabase()
                .HashGetAsync(ConfigKeyName, [WalEndField, MessageNoField], CommandFlags.DemandMaster)
                .ConfigureAwait(false);

            // no config yet - a fresh setup
            if (values[0].IsNull && values[1].IsNull) return (0, 0);

            var walEndValue = values[0].IsNull ? null : values[0].ToString();
            var messageNoValue = values[1].IsNull ? null : values[1].ToString();

            if (!ulong.TryParse(walEndValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out var walEnd)
                || !ulong.TryParse(messageNoValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out var messageNo))
            {
                throw new Exception($"Missing or invalid WAL end LSN in the config of stream '{_options.StreamName}'");
            }

            return (walEnd, messageNo);
        }

        private string ConfigKeyName => string.Join(':', _options.StreamName, ConfigKeySuffix);

        private void DisposeTasks()
        {
            _publishedTasks.TryDispose(_logger);
            _publishedTasks.Clear();
        }

        public override async ValueTask DisposeAsync()
        {
            DisposeTasks();

            await _redis.TryDisposeAsync(_logger)
                .ConfigureAwait(false);
        }

        private ConnectionMultiplexer? _redis;

        private List<Task> _publishedTasks = [];

        private readonly RedisPublisherOptions _options;
        private readonly ILogger<RedisPublisher>? _logger;

        private readonly bool _useDeduplication;

        private ulong _lastWal;
        private ulong _lastMessageNo;
    }
}