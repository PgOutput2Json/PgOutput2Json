using Microsoft.Extensions.Logging;
using PgOutput2Json.Core;
using PgOutput2Json.RabbitMq;

using var loggerFactory = LoggerFactory.Create(builder => 
    builder.AddFilter("PgOutput2Json", LogLevel.Debug)
           .AddFilter("Microsoft", LogLevel.Warning)
           .AddSimpleConsole(options =>
           {
               options.SingleLine = true;
               options.TimestampFormat = "hh:mm:ss ";
           }));

var options = new ReplicationListenerOptions(
    "server=localhost;database=repl_test_db;username=replicator;password=replicator", "pub_test", "test_slot")
{
    KeyColumns = new Dictionary<string, KeyColumn>
    {
        { "public.tab_test", new KeyColumn(5, "id", "name" ) }
    }
};

using var listener = new ReplicationListener(options,
    logger: loggerFactory.CreateLogger<ReplicationListener>());

using var publisher = new MessagePublisher(new[] { "localhost" }, "guest", "guest", 
    logger: loggerFactory.CreateLogger<MessagePublisher>());

var forwarder = new MessageForwarder(listener, publisher, 3);

var cancellationTokenSource = new CancellationTokenSource();

await forwarder.Start(cancellationTokenSource.Token);
