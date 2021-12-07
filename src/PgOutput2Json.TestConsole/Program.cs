// See https://aka.ms/new-console-template for more information
using PgOutput2Json.Core;
using PgOutput2Json.RabbitMq;

var options = new ReplicationListenerOptions(
    "server=localhost;database=repl_test_db;username=replicator;password=replicator",
    "pub_test",
    "test_slot");

options.KeyColumns = new Dictionary<string, KeyColumn>
    {
        { "public.tab_test", new KeyColumn(5, "id", "name" ) }
    };


using var listener = new ReplicationListener(options);

listener.LoggingInfoHandler += msg => Console.WriteLine(msg);
listener.LoggingWarnHandler += msg => Console.WriteLine(msg);
listener.LoggingErrorHandler += (ex, msg) =>
    {
        Console.WriteLine(msg);
        Console.WriteLine(ex.ToString());
    };


using var publisher = new MessagePublisher(new[] { "localhost" }, "guest", "guest");

var forwarder = new MessageForwarder(listener, publisher, 3);

var cancellationTokenSource = new CancellationTokenSource();

await forwarder.Start(cancellationTokenSource.Token);
