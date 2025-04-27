using System;
using System.Threading;
using System.Threading.Tasks;

namespace PgOutput2Json
{
    public interface IMessagePublisher: IAsyncDisposable
    {
        Task PublishAsync(ulong walSeqNo, string json, string tableName, string keyColumnValue, int partition, CancellationToken token);
        Task ConfirmAsync(CancellationToken token);

        Task<ulong> GetLastPublishedWalSeq(CancellationToken token);
    }

    public abstract class MessagePublisher : IMessagePublisher
    {
        public abstract Task PublishAsync(ulong walSeqNo, string json, string tableName, string keyColumnValue, int partition, CancellationToken token);
        public abstract Task ConfirmAsync(CancellationToken token);
        public abstract ValueTask DisposeAsync();

        public virtual Task<ulong> GetLastPublishedWalSeq(CancellationToken token)
        {
            return Task.FromResult<ulong>(0);
        }
    }
}
