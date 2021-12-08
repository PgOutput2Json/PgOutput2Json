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

using var pgOutput2Json = PgOutput2JsonBuilder.Create()
    .WithLoggerFactory(loggerFactory)
    .WithPgConnectionString("server=localhost;database=repl_test_db;username=replicator;password=replicator")
    .WithPgPublications("pub_test")
    .WithPgReplicationSlot("test_slot")
    .WithPgKeyColumn("public.tab_test", 5, "id", "name")
    .UseRabbitMq(options =>
    {
        options.HostNames = new[] { "localhost" };
        options.Username = "guest";
        options.Password = "guest";
    })
    .Build();

var cancellationTokenSource = new CancellationTokenSource();

await pgOutput2Json.Start(cancellationTokenSource.Token);
