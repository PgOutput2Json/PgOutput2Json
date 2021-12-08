
namespace PgOutput2Json.Core
{
    public interface IPgOutput2Json: IDisposable
    {
        Task Start(CancellationToken cancellationToken);
    }
}