using System;

namespace PgOutput2Json
{
    public interface IMessagePublisher: IDisposable
    {
        void Publish(string json, string tableName, string keyColumnValue, int partition);
        void WaitForConfirmsOrDie(TimeSpan timeout);
    }
}
