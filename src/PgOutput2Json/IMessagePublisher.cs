using System;
using System.Threading;
using System.Threading.Tasks;

using Npgsql.Replication.PgOutput.Messages;

namespace PgOutput2Json
{
    public interface IMessagePublisher: IAsyncDisposable
    {
        Task PublishAsync(ulong walSeqNo, string json, string tableName, string keyColumnValue, int partition, CancellationToken token);
        Task ConfirmAsync(CancellationToken token);

        Task<ulong> GetLastPublishedWalSeq(CancellationToken token);

        Task HandleRelationMessage(RelationMessage message, CancellationToken token);
    }
}
