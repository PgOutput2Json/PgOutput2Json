using System;
using System.Net.Http;
using System.Threading;

using Microsoft.Extensions.Logging;

namespace PgOutput2Json.ClickHouse
{
    internal class ClickHousePublisherFactory : IMessagePublisherFactory
    {
        private readonly ClickHousePublisherOptions _options;

        // shared by the publishers: the main stream and the sync workers
        private readonly TargetNames _names;

        // one client for the publishers of the main stream and of the table syncs
        private readonly Lazy<ClickHouseClient> _client;

        public ClickHousePublisherFactory(ClickHousePublisherOptions options)
        {
            _options = options;
            _names = new TargetNames(options.GetTableName);

            _client = new Lazy<ClickHouseClient>(
                () => new ClickHouseClient(options, options.HttpClient ?? new HttpClient { Timeout = Timeout.InfiniteTimeSpan }),
                LazyThreadSafetyMode.ExecutionAndPublication);
        }

        // the compact write mode has the column types, and the JSON is not seen outside the output
        public JsonWriteMode? RequiredWriteMode => JsonWriteMode.Compact;

        public IMessagePublisher CreateMessagePublisher(ReplicationListenerOptions listenerOptions, string slotName, ILoggerFactory? loggerFactory)
        {
            return new ClickHousePublisher(_options, _client.Value, loggerFactory?.CreateLogger<ClickHousePublisher>()) { Names = _names };
        }
    }
}
