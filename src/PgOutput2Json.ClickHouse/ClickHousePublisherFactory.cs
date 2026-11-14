using System;
using System.Net.Http;
using System.Threading;

using Microsoft.Extensions.Logging;

namespace PgOutput2Json.ClickHouse
{
    internal class ClickHousePublisherFactory : IMessagePublisherFactory
    {
        private readonly ClickHousePublisherOptions _options;

        // one client for the publishers of the main stream and of the table syncs
        private readonly Lazy<ClickHouseClient> _client;

        public ClickHousePublisherFactory(ClickHousePublisherOptions options)
        {
            _options = options;

            _client = new Lazy<ClickHouseClient>(
                () => new ClickHouseClient(options, options.HttpClient ?? new HttpClient { Timeout = Timeout.InfiniteTimeSpan }),
                LazyThreadSafetyMode.ExecutionAndPublication);
        }

        // the compact write mode has the column types, and the JSON is not seen outside the output
        public JsonWriteMode? RequiredWriteMode => JsonWriteMode.Compact;

        public IMessagePublisher CreateMessagePublisher(ReplicationListenerOptions listenerOptions, string slotName, ILoggerFactory? loggerFactory)
        {
            return new ClickHousePublisher(_options, _client.Value, loggerFactory?.CreateLogger<ClickHousePublisher>());
        }
    }
}
