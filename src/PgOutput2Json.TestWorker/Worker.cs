using PgOutput2Json.RabbitMq;
using PgOutput2Json.Redis;

namespace PgOutput2Json.TestWorker
{
    public class Worker : BackgroundService
    {
        private readonly ILoggerFactory _loggerFactory;

        private readonly int _batchSize;
        private readonly int _filterFrom;
        private readonly int _filterTo;

        public Worker(ILoggerFactory loggerFactory, IConfiguration configuration)
        {
            _loggerFactory = loggerFactory;
            _batchSize = configuration.GetValue<int>("AppSettings:BatchSize");
            _filterFrom = configuration.GetValue<int>("AppSettings:PartitionFilter:FromInclusive");
            _filterTo = configuration.GetValue<int>("AppSettings:PartitionFilter:ToExclusive");
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            using var pgOutput2Json = PgOutput2JsonBuilder.Create()
                .WithLoggerFactory(_loggerFactory)
                .WithPgConnectionString("server=localhost;database=repl_test_db;username=replicator;password=replicator")
                .WithPgPublications("pub_test")
                .WithPgKeyColumn("public.tab_test", 5, "id")
                .WithBatchSize(250)
                .WithJsonOptions(options =>
                {
                    //options.WriteNulls = true;
                    //options.WriteTimestamps = true;
                    //options.WriteTableNames = true;
                })
                .WithPartitionFilter(_filterFrom, _filterTo)
                .WithBatchSize(_batchSize)
                .WithMessageHandler((json, table, key, partition) =>
                {
                    Console.WriteLine($"{table}: {json}");
                })
                //.UseRabbitMq(options =>
                //{
                //    options.UsePersistentMessagesByDefault = false;
                //})
                .UseRedis(options =>
                {
                    options.EndPoints.Add("localhost:6379");
                })
                .Build();

            await pgOutput2Json.Start(stoppingToken);
        }
    }
}