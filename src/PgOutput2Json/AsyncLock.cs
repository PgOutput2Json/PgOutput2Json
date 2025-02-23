using System;
using System.Threading;
using System.Threading.Tasks;

namespace PgOutput2Json
{
    class AsyncLock
    {
        private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1, 1);

        public async Task<IDisposable> LockAsync(CancellationToken token)
        {
            await _semaphore.WaitAsync(token).ConfigureAwait(false);

            return new Lock(_semaphore);
        }

        private class Lock : IDisposable
        {
            private readonly SemaphoreSlim _semaphore;

            public Lock(SemaphoreSlim semaphore) => _semaphore = semaphore;

            public void Dispose() => _semaphore.Release();
        }
    }
}
