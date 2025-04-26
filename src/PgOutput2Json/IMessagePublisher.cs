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
        public string? LastJson { get; set; }

        /// <summary>
        /// This is optionally populated by the client app, not used by the publisher.
        /// It allows the client do decide where to continue the copy process, based on the lastJson exported
        /// </summary>
        public string? AdditionalRowFilter { get; set; }

        /// <summary>
        /// This is optionally populated by the client app, not used by the publisher.
        /// It is used in compbination with AdditionalRowFilter, and allows the client do decide where to continue the copy process, 
        /// based on the lastJson exported
        /// </summary>
        public string? OrderByColumns { get; set; }
    }
}
