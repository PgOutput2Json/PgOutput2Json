using PgOutput2Json.RabbitMq;

namespace PgOutput2Json.TestWorker
{
    public class Worker : BackgroundService
    {
        private readonly ILoggerFactory _loggerFactory;

        public Worker(ILoggerFactory loggerFactory)
        {
            _loggerFactory = loggerFactory;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            using var pgOutput2Json = PgOutput2JsonBuilder.Create()
                .WithLoggerFactory(_loggerFactory)
                .WithPgConnectionString("server=localhost;database=repl_test_db;username=replicator;password=replicator")
                .WithPgPublications("pub_test")
                .WithPgReplicationSlot("test_slot")
                .UseRabbitMq(options =>
                {
                    options.HostNames = new[] { "localhost" };
                    options.Username = "guest";
                    options.Password = "guest";
                })
                .Build();

            await pgOutput2Json.Start(stoppingToken);
        }
    }
}