using System;

namespace PgOutput2Json
{
    public interface IMessagePublisher: IDisposable
    {
        bool Publish(string json, string tableName, string keyColumnValue, int partition);
        void ForceConfirm();
    }
}
