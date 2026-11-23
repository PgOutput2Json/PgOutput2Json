using Microsoft.Extensions.Logging;

namespace PgOutput2Json.Benchmark
{
    /// <summary>
    /// Wraps the factory of an output (or none, for the null output) and counts the rows of one table that
    /// the output has confirmed. The benchmark is done when all of them are, so it works the same for every
    /// output, and nothing polls the target.
    /// </summary>
    internal sealed class CountingPublisherFactory(IMessagePublisherFactory? inner, string tableName, long expectedRows) : IMessagePublisherFactory
    {
        private readonly string _tableName = tableName;

        private long _confirmed;

        public TaskCompletionSource Done { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public long Confirmed => Interlocked.Read(ref _confirmed);

        public JsonWriteMode? RequiredWriteMode => inner?.RequiredWriteMode;

        public bool SupportsParallelCopy => inner?.SupportsParallelCopy ?? true;

        public void Validate(ReplicationListenerOptions listenerOptions) => inner?.Validate(listenerOptions);

        public IMessagePublisher CreateMessagePublisher(ReplicationListenerOptions listenerOptions, string slotName, ILoggerFactory? loggerFactory)
        {
            var publisher = inner?.CreateMessagePublisher(listenerOptions, slotName, loggerFactory);

            // an output that takes the rows gets them through the wrapper too
            return publisher is IRowPublisher rowPublisher ? new RowPublisher(this, rowPublisher) : new Publisher(this, publisher);
        }

        private void AddConfirmed(long rows)
        {
            if (Interlocked.Add(ref _confirmed, rows) >= expectedRows) Done.TrySetResult();
        }

        private sealed class RowPublisher(CountingPublisherFactory owner, IRowPublisher inner) : Publisher(owner, inner), IRowPublisher
        {
            public async Task PublishRowAsync(ChangeRow row, CancellationToken token)
            {
                await inner.PublishRowAsync(row, token).ConfigureAwait(false);

                if (row.Table.Name == Owner._tableName) Unconfirmed++;
            }
        }

        private class Publisher(CountingPublisherFactory owner, IMessagePublisher? inner) : MessagePublisher
        {
            protected CountingPublisherFactory Owner => owner;

            protected long Unconfirmed { get => _unconfirmed; set => _unconfirmed = value; }

            private long _unconfirmed;

            public override async Task PublishAsync(JsonMessage jsonMessage, CancellationToken token)
            {
                if (inner != null) await inner.PublishAsync(jsonMessage, token).ConfigureAwait(false);

                // the table name is a StringBuilder: compared without making a string of it
                if (jsonMessage.TableName == owner._tableName) _unconfirmed++;
            }

            public override async Task ConfirmAsync(CancellationToken token)
            {
                if (inner != null) await inner.ConfirmAsync(token).ConfigureAwait(false);

                if (_unconfirmed == 0) return;

                owner.AddConfirmed(_unconfirmed);
                _unconfirmed = 0;
            }

            public override Task<(ulong, ulong)> GetLastPublishedWalSeqAsync(CancellationToken token)
            {
                return inner?.GetLastPublishedWalSeqAsync(token) ?? Task.FromResult<(ulong, ulong)>((0, 0));
            }

            public override ValueTask DisposeAsync()
            {
                return inner?.DisposeAsync() ?? ValueTask.CompletedTask;
            }
        }
    }
}
