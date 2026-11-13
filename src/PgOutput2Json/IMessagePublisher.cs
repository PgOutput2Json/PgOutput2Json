using System;
using System.Threading;
using System.Threading.Tasks;

namespace PgOutput2Json
{
    public interface IMessagePublisher: IAsyncDisposable
    {
        Task PublishAsync(JsonMessage jsonMessage, CancellationToken token);
        Task ConfirmAsync(CancellationToken token);

        /// <summary>
        /// Returns the position ("w", "n") of the last change the publisher stored, so the listener can skip
        /// the changes PostgreSQL replays after a reconnect. Only publishers that save the position together
        /// with the data implement this. The default (0, 0) means nothing is skipped: delivery is at-least-once
        /// and consumers deduplicate. A publisher that implements this must not save the position of the
        /// messages marked with <see cref="JsonMessage.IsTableSync"/>.
        /// </summary>
        Task<(ulong, ulong)> GetLastPublishedWalSeqAsync(CancellationToken token)
        {
            return Task.FromResult<(ulong, ulong)>((0, 0));
        }
    }

    public abstract class MessagePublisher : IMessagePublisher
    {
        public abstract Task PublishAsync(JsonMessage jsonMessage, CancellationToken token);
        public abstract Task ConfirmAsync(CancellationToken token);
        public abstract ValueTask DisposeAsync();

        public virtual Task<(ulong, ulong)> GetLastPublishedWalSeqAsync(CancellationToken token)
        {
            return Task.FromResult<(ulong, ulong)>((0, 0));
        }
    }
}
