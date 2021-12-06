// See https://aka.ms/new-console-template for more information
using PgOutput2Json.Core;

var options = new ReplicationListenerOptions
{
    WriteNulls = false,
    LoggingInfoHandler = msg => Console.WriteLine(msg),
    LoggingWarnHandler = msg => Console.WriteLine(msg), 
    LoggingErrorHandler = (ex, msg) =>
    {
        Console.WriteLine(msg);
        Console.WriteLine(ex.ToString());
    }
};

var listener = new ReplicationListener(options);

var cancellationTokenSource = new CancellationTokenSource();

await listener.ListenForChanges(cancellationTokenSource.Token);
