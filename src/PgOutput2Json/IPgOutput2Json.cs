namespace PgOutput2Json
{
    public interface IPgOutput2Json: IDisposable
    {
        Task Start(CancellationToken cancellationToken);
    }
}