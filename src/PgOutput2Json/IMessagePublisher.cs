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

        Task ReportDataCopyProgress(string tableName, string lastJson, CancellationToken token);
        Task ReportDataCopyCompleted(string tableName, CancellationToken token);
        
        Task<DataCopyStatus> GetDataCopyStatus(string tableName, CancellationToken token);
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

        public virtual Task ReportDataCopyProgress(string tableName, string lastJson, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public virtual Task ReportDataCopyCompleted(string tableName, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public virtual Task<DataCopyStatus> GetDataCopyStatus(string tableName, CancellationToken token)
        {
            throw new NotImplementedException();
        }
    }

    public class DataCopyStatus
    {
        public bool IsCompleted { get; set; }
        public string? AdditionalRowFilter { get; set; }
        public string? OrderByColumns { get; set; }
    }
}
