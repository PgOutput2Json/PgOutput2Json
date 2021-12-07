// See https://aka.ms/new-console-template for more information
using PgOutput2Json.Core;
using PgOutput2Json.RabbitMq;

var options = new ReplicationListenerOptions(
    "server=localhost;database=repl_test_db;username=replicator;password=replicator",
    "pub_test",
    "test_slot"); 

options.Partitions = new Dictionary<string, PartionConfig>
    {
        { "public.tab_test", new PartionConfig("id", 5) }
    };

var listener = new ReplicationListener(options);

listener.LoggingInfoHandler += msg => Console.WriteLine(msg);
listener.LoggingWarnHandler += msg => Console.WriteLine(msg);
listener.LoggingErrorHandler += (ex, msg) =>
    {
        Console.WriteLine(msg);
        Console.WriteLine(ex.ToString());
    };


var cancellationTokenSource = new CancellationTokenSource();

var publisher = new MessagePublisher(new[] { "localhost" }, "guest", "guest");

var forwarder = new MessageForwarder(listener, publisher, 100, 2);

await forwarder.Start(cancellationTokenSource.Token);
