using System;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PgOutput2Json
{
    public class PgOutput2JsonBuilder
    {
        private IMessagePublisherFactory? _messagePublisherFactory;
        private ILoggerFactory? _loggerFactory;

        private readonly JsonOptions _jsonOptions = new();
        private readonly ReplicationListenerOptions _listenerOptions = new();

        public static PgOutput2JsonBuilder Create()
        {
            return new PgOutput2JsonBuilder();
        }

        public PgOutput2JsonBuilder WithPgConnectionString(string connectionString)
        {
            _listenerOptions.DataSourceBuilder.ConnectionStringBuilder.ConnectionString = connectionString;
            return this;
        }

        public PgOutput2JsonBuilder WithPgDataSource(Action<NpgsqlDataSourceBuilder> builderAction)
        {
            builderAction(_listenerOptions.DataSourceBuilder);
            return this;
        }

        public PgOutput2JsonBuilder WithPgReplicationSlot(string replicationSlotName, bool useTemporarySlot = false)
        {
            _listenerOptions.ReplicationSlotName = replicationSlotName;
            _listenerOptions.UseTemporarySlot = useTemporarySlot;
            return this;
        }

        public PgOutput2JsonBuilder WithPgPublications(params string[] publicationNames)
        {
            _listenerOptions.PublicationNames = publicationNames;
            return this;
        }

        /// <param name="columnNames"></param>
        /// <returns></returns>
        public PgOutput2JsonBuilder WithPartitionKeyFields(string tableName, string field, params string[] additionalFields)
        {
            _listenerOptions.PartitionKeyColumns[tableName] = [ field, .. additionalFields];
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
            _listenerOptions.IncludedColumns[tableName] = columnNames;
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
            _listenerOptions.OrderedKeys[tableName] = orderedKeyColumns;
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

        public PgOutput2JsonBuilder WithBatchSize(int batchSize)
        {
            _listenerOptions.BatchSize = batchSize;
            return this;
        }

        public PgOutput2JsonBuilder WithLoggerFactory(ILoggerFactory loggerFactory)
        {
            _listenerOptions.DataSourceBuilder.UseLoggerFactory(loggerFactory);
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
            _listenerOptions.CopyData = copyData;
            _listenerOptions.MaxParallelCopyJobs = maxParallelJobs;
            _listenerOptions.CopyDataBatchSize = copyDataBatchSize;
            return this;
        }

        public PgOutput2JsonBuilder WithDeduplication(bool useDeduplication)
        {
            _listenerOptions.UseDeduplication = useDeduplication;
            return this;
        }

        public IPgOutput2Json Build()
        {
            if (_listenerOptions.DataSourceBuilder.ConnectionString == string.Empty)
            {
                throw new ArgumentNullException("PostgreSQL data source must be configured");
            }

            if (_listenerOptions.PublicationNames == null || _listenerOptions.PublicationNames.Length == 0)
            {
                throw new ArgumentNullException("At least one PostgreSQL publication name must be provided");
            }

            if (_messagePublisherFactory == null)
            {
                throw new ArgumentNullException("MessagePublisherFactory must be provided");
            }

            if (_listenerOptions.BatchSize <= 0)
            {
                throw new ArgumentOutOfRangeException("Batch size must be greater than zero");
            }

            // if not explicitely specified in builder, use temporary slot only if slot name is not provided

            if (!_listenerOptions.UseTemporarySlot && string.IsNullOrWhiteSpace(_listenerOptions.ReplicationSlotName))
            {
                throw new ArgumentOutOfRangeException("Replication slot name must be provided for permanent slots");
            }

            _listenerOptions.DataSourceBuilder.ConnectionStringBuilder.Timezone ??= "UTC";

            if (_listenerOptions.MaxParallelCopyJobs <= 0)
            {
                _listenerOptions.MaxParallelCopyJobs = 1;
            }

            if (_listenerOptions.BatchWaitTime < TimeSpan.FromMilliseconds(10))
            {
                _listenerOptions.BatchWaitTime = TimeSpan.FromMilliseconds(10);
            }

            var listener = new ReplicationListener(_messagePublisherFactory, _listenerOptions, _jsonOptions, _loggerFactory);

            var pgOutput2Json = new PgOutput2Json(listener, _loggerFactory);

            return pgOutput2Json;
        }

    }
}
