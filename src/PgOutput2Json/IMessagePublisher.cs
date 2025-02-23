using System;
using System.Threading;
using System.Threading.Tasks;

namespace PgOutput2Json
{
    public interface IMessagePublisher: IAsyncDisposable
    {
        Task<bool> PublishAsync(string json, string tableName, string keyColumnValue, int partition, CancellationToken token);
        Task ForceConfirmAsync(CancellationToken token);
    }
}
