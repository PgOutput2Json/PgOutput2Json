using System;
using System.Threading;
using System.Threading.Tasks;

namespace PgOutput2Json
{
    public interface IMessagePublisher: IAsyncDisposable
    {
        Task PublishAsync(JsonMessage jsonMessage, CancellationToken token);
        Task ConfirmAsync(CancellationToken token);

        Task<ulong> GetLastPublishedWalSeqAsync(CancellationToken token);
    }

    public abstract class MessagePublisher : IMessagePublisher
    {
        public abstract Task PublishAsync(JsonMessage jsonMessage, CancellationToken token);
        public abstract Task ConfirmAsync(CancellationToken token);
        public abstract ValueTask DisposeAsync();

        public virtual Task<ulong> GetLastPublishedWalSeqAsync(CancellationToken token)
        {
            return Task.FromResult<ulong>(0);
        }
    }
}
