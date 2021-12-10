using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;

namespace PgOutput2Json
{
    public class PgOutput2JsonBuilder
    {
        private string? _connectionString;
        private string _replicationSlotName = string.Empty;
        private string[]? _publicationNames;
        private Dictionary<string, KeyColumn> _keyColumns = new Dictionary<string, KeyColumn>();
        private IMessagePublisherFactory? _messagePublisherFactory;
        private int _batchSize = 100;
        private int _confirmTimeoutSec = 30;
        private ILoggerFactory? _loggerFactory;
        private JsonOptions _jsonOptions = new JsonOptions();
        private PartitionFilter? _partitionFilter;

        public static PgOutput2JsonBuilder Create()
        {
            return new PgOutput2JsonBuilder();
        }

        public PgOutput2JsonBuilder WithPgConnectionString(string connectionString)
        {
            _connectionString = connectionString;
            return this;
        }

        public PgOutput2JsonBuilder WithPgReplicationSlot(string replicationSlotName)
        {
            _replicationSlotName = replicationSlotName;
            return this;
        }

        public PgOutput2JsonBuilder WithPgPublications(params string[] publicationNames)
        {
            _publicationNames = publicationNames;
            return this;
        }

        public PgOutput2JsonBuilder WithPgKeyColumn(string tableName, string keyColumn, int partitionsCount = 1)
        {
            _keyColumns[tableName] = new KeyColumn(partitionsCount, keyColumn);
            return this;
        }

        public PgOutput2JsonBuilder WithPgKeyColumn(string tableName, int partitionsCount, params string[] columnNames)
        {
            _keyColumns[tableName] = new KeyColumn(partitionsCount, columnNames);
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

        public PgOutput2JsonBuilder WithBatchSize(int batchSize)
        {
            _batchSize = batchSize;
            return this;
        }

        public PgOutput2JsonBuilder WithConfirmTimeoutSec(int confirmTimeoutSec)
        {
            _confirmTimeoutSec = confirmTimeoutSec;
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

        public IPgOutput2Json Build()
        {
            if (string.IsNullOrWhiteSpace(_connectionString)) 
                throw new ArgumentNullException("PostgreSQL connection string must be provided");

            if (_publicationNames == null || _publicationNames.Length == 0)
                throw new ArgumentNullException("At least one PostgreSQL publication name must be provided");

            if (_messagePublisherFactory == null)
                throw new ArgumentNullException("MessagePublisherFactory must be provided");

            if (_batchSize <= 0)
                throw new ArgumentOutOfRangeException("Publishing BatchSize must be greater than zero");

            if (_confirmTimeoutSec <= 0)
                throw new ArgumentOutOfRangeException("Publishing ConfirmTimeoutSec must be greater than zero");

            var options = new ReplicationListenerOptions(_connectionString, _replicationSlotName, _publicationNames, _partitionFilter);
            options.KeyColumns = new Dictionary<string, KeyColumn>(_keyColumns);

            var listener = new ReplicationListener(options, _jsonOptions, _loggerFactory?.CreateLogger<ReplicationListener>());

            var messagePublisher = _messagePublisherFactory.CreateMessagePublisher(_loggerFactory);

            var pgOutput2Json = new PgOutput2Json(listener, messagePublisher, _batchSize, _confirmTimeoutSec);

            return pgOutput2Json;
        }

    }
}
