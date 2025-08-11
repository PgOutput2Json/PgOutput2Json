using System;
using System.Threading;
using System.Threading.Tasks;

namespace PgOutput2Json
{
    public interface IPgOutput2Json: IDisposable
    {
        Task StartAsync(CancellationToken cancellationToken);
        /*
        Task<bool> WhenReplicationStartsAsync(TimeSpan timeout, CancellationToken cancellationToken);
        Task<bool> WhenLsnReachesAsync(string expectedLsn, TimeSpan timeout, CancellationToken cancellationToken);
        */
    }
}