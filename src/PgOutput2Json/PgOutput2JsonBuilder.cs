using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PgOutput2Json
{
    public class PgOutput2JsonBuilder
    {
        private string? _connectionString;
        private string _replicationSlotName = string.Empty;
        private string[]? _publicationNames;
        private Dictionary<string, int> _tablePartitions = new Dictionary<string, int>();
        private Dictionary<string, IReadOnlyList<string>> _columns = new Dictionary<string, IReadOnlyList<string>>();
        private Dictionary<string, IReadOnlyList<string>> _orderedKeys = new Dictionary<string, IReadOnlyList<string>>();
        private IMessagePublisherFactory? _messagePublisherFactory;
        private int _idleFlushTimeSec = 2;
        private ILoggerFactory? _loggerFactory;
        private JsonOptions _jsonOptions = new JsonOptions();
        private PartitionFilter? _partitionFilter;
        private bool _useTemporarySlot = true;
        private int _batchSize = 100;

        private bool _copyData = false;
        private int _copyDatsBatchSize = 0;
        private int _maxParallelCopyJobs = 1;

        public static PgOutput2JsonBuilder Create()
        {
            return new PgOutput2JsonBuilder();
        }

        public PgOutput2JsonBuilder WithPgConnectionString(string connectionString)
        {
            _connectionString = connectionString;
            return this;
        }

        public PgOutput2JsonBuilder WithPgReplicationSlot(string replicationSlotName, bool useTemporarySlot = false)
        {
            _replicationSlotName = replicationSlotName;
            _useTemporarySlot = useTemporarySlot;
            return this;
        }

        public PgOutput2JsonBuilder WithPgPublications(params string[] publicationNames)
        {
            _publicationNames = publicationNames;
            return this;
        }

        /// <param name="columnNames"></param>
        /// <returns></returns>
        public PgOutput2JsonBuilder WithTablePartitions(string tableName, int partitionsCount)
        {
            if (partitionsCount < 1) throw new ArgumentOutOfRangeException(nameof(partitionsCount));

            _tablePartitions[tableName] = partitionsCount;
            return this;
        }

        /// <summary>
        /// Specifies which table columns to include in the JSON output. 
        /// If a table is not specified at all, then all columns are included in the output.
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="columnNames"></param>
        /// <returns></returns>
        public PgOutput2JsonBuilder WithPgColumns(string tableName, params string[] columnNames)
        {
            _columns[tableName] = columnNames;
            return this;
        }

        /// <summary>
        /// Specifies which table columns to are to be used for ordering during the initial data copy, AND for resuming an interrupted initial data copy.
        /// If a table is not specified at all, then no ordering will be used during the initial data copy, and resuming will not be supported.
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="orderedKeyColumns"></param>
        /// <returns></returns>
        public PgOutput2JsonBuilder WithPgOrderedKeyColumns(string tableName, params string[] orderedKeyColumns)
        {
            _orderedKeys[tableName] = orderedKeyColumns;
            return this;
        }

        public PgOutput2JsonBuilder WithPartitionFilter(int fromInclusive, int toExclusive)
        {
            _partitionFilter = new PartitionFilter(fromInclusive, toExclusive);
            return this;
        }

        public PgOutput2JsonBuilder WithMessagePublisherFactory(IMessagePublisherFactory messagePublisherFactory)
        {
            _messagePublisherFactory = messagePublisherFactory;
            return this;
        }

        public PgOutput2JsonBuilder WithMessageHandler(SimpleMessageHandler messageHandler)
        {
            _messagePublisherFactory = new SimpleMessagePublisherFactory(messageHandler);
            return this;
        }

        public PgOutput2JsonBuilder WithIdleFlushTime(int idleFlushTimeSec)
        {
            _idleFlushTimeSec = idleFlushTimeSec;
            return this;
        }

        public PgOutput2JsonBuilder WithBatchSize(int batchSize)
        {
            _batchSize = batchSize;
            return this;
        }

        public PgOutput2JsonBuilder WithLoggerFactory(ILoggerFactory loggerFactory)
        {
            _loggerFactory = loggerFactory;
            return this;
        }

        public PgOutput2JsonBuilder WithJsonOptions(Action<JsonOptions> configureAction)
        {
            configureAction.Invoke(_jsonOptions);
            return this;
        }

        public PgOutput2JsonBuilder WithInitialDataCopy(bool copyData = false, int maxParallelJobs = 1, int copyDataBatchSize = 0)
        {
            _copyData = copyData;
            _maxParallelCopyJobs = maxParallelJobs;
            _copyDatsBatchSize = copyDataBatchSize;
            return this;
        }

        public IPgOutput2Json Build()
        {
            if (string.IsNullOrWhiteSpace(_connectionString)) 
                throw new ArgumentNullException("PostgreSQL connection string must be provided");

            if (_publicationNames == null || _publicationNames.Length == 0)
                throw new ArgumentNullException("At least one PostgreSQL publication name must be provided");

            if (_messagePublisherFactory == null)
                throw new ArgumentNullException("MessagePublisherFactory must be provided");

            if (_idleFlushTimeSec <= 0)
                throw new ArgumentOutOfRangeException("Idle flush time must be greater than zero");

            if (_batchSize <= 0)
                throw new ArgumentOutOfRangeException("Idle flush time must be greater than zero");

            // if not explicitely specified in builder use temporary slot only if slot name is not provided

            if (!_useTemporarySlot && string.IsNullOrWhiteSpace(_replicationSlotName))
            {
                throw new ArgumentOutOfRangeException("Replication slot name must be provided for permanent slots");
            }

            var cnStringBuilder = new NpgsqlConnectionStringBuilder(_connectionString);

            cnStringBuilder.Timezone ??= "UTC";

            if (_maxParallelCopyJobs <= 0) _maxParallelCopyJobs = 1;

            var options = new ReplicationListenerOptions(cnStringBuilder.ConnectionString,
                                                         _useTemporarySlot,
                                                         _replicationSlotName,
                                                         _publicationNames,
                                                         TimeSpan.FromSeconds(_idleFlushTimeSec),
                                                         _batchSize,
                                                         _tablePartitions,
                                                         _columns,
                                                         _orderedKeys,
                                                         _partitionFilter,
                                                         _copyData,
                                                         _copyDatsBatchSize,
                                                         _maxParallelCopyJobs);

            var listener = new ReplicationListener(_messagePublisherFactory, options, _jsonOptions, _loggerFactory);

            var pgOutput2Json = new PgOutput2Json(listener, _loggerFactory);

            return pgOutput2Json;
        }

    }
}
