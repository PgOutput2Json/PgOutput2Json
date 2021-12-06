// See https://aka.ms/new-console-template for more information
using PgOutput2Json.Core;

int count = 0;

var options = new ReplicationListenerOptions("server=localhost;database=repl_test_db;username=replicator;password=replicator", 
    "pub_test", 
    "test_slot", 
    (json, tableName, partition) =>
    {
        Console.WriteLine(tableName + "." + partition);
        Console.WriteLine(json);

        if (++count >= 2)
        {
            count = 0;
            return true;
        }

        return false;
    });

options.Partitions = new Dictionary<string, PartionConfig>
    {
        { "public.tab_test", new PartionConfig("id", 5) }
    };

options.LoggingInfoHandler = msg => Console.WriteLine(msg);

options.LoggingWarnHandler = msg => Console.WriteLine(msg);

options.LoggingErrorHandler = (ex, msg) =>
    {
        Console.WriteLine(msg);
        Console.WriteLine(ex.ToString());
    };

var listener = new ReplicationListener(options);

var cancellationTokenSource = new CancellationTokenSource();

await listener.ListenForChanges(cancellationTokenSource.Token);
