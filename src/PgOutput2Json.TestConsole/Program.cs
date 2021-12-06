// See https://aka.ms/new-console-template for more information
using PgOutput2Json.Core;

var options = new ReplicationListenerOptions
{
    WriteNulls = false,
    Partitions = new Dictionary<string, PartionConfig>
    {
        { "public.tab_test", new PartionConfig("id", 5) }
    },

    LoggingInfoHandler = msg => Console.WriteLine(msg),
    LoggingWarnHandler = msg => Console.WriteLine(msg), 
    LoggingErrorHandler = (ex, msg) =>
    {
        Console.WriteLine(msg);
        Console.WriteLine(ex.ToString());
    },

    MessageHandler = (json, tableName, partition) =>
    {
        Console.WriteLine(tableName + "." + partition);
        Console.WriteLine(json);
    }
};

var listener = new ReplicationListener(options);

var cancellationTokenSource = new CancellationTokenSource();

await listener.ListenForChanges(cancellationTokenSource.Token);
