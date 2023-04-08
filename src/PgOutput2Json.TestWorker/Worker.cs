using PgOutput2Json.RabbitMq;
using PgOutput2Json.Redis;

namespace PgOutput2Json.TestWorker
{
    public class Worker : BackgroundService
    {
        private class PartitionInfo
        {
            public string? Table { get; set; }
            public int? PartitionCount { get; set; }
            public string[]? Columns { get; set; }
        };

        private readonly ILoggerFactory _loggerFactory;

        private readonly int? _batchSize;
        private readonly int? _filterFrom;
        private readonly int? _filterTo;
        private readonly string[] _publicationNames;
        private readonly string? _connectionString;
        private readonly PartitionInfo[] _partitions;

        public Worker(ILoggerFactory loggerFactory, IConfiguration configuration)
        {
            _loggerFactory = loggerFactory;
            _connectionString = configuration.GetConnectionString("PublicationDatabase");
            _publicationNames = configuration.GetSection("AppSettings:PublicationNames").Get<string[]>() ?? Array.Empty<string>();
            _batchSize = configuration.GetSection("AppSettings:BatchSize").Get<int?>();
            _filterFrom = configuration.GetSection("AppSettings:PartitionFilter:FromInclusive").Get<int?>();
            _filterTo = configuration.GetSection("AppSettings:PartitionFilter:ToExclusive").Get<int?>();
            _partitions = configuration.GetSection("AppSettings:Partitions").Get<PartitionInfo[]>() ?? Array.Empty<PartitionInfo>();
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            if (_connectionString == null) throw new Exception("Missing connection string");
            if (_publicationNames.Length == 0) throw new Exception("Missing publication names");

            var builder = PgOutput2JsonBuilder.Create()
                .WithLoggerFactory(_loggerFactory)
                .WithPgConnectionString(_connectionString)
                .WithPgPublications(_publicationNames)
                .WithJsonOptions(options =>
                {
                    //options.WriteNulls = true;
                    //options.WriteTimestamps = true;
                    //options.WriteTableNames = true;
                })
                .WithMessageHandler((json, table, key, partition) =>
                {
                    Console.WriteLine($"{table}: {json}");
                })
                //.UseRabbitMq(options =>
                //{
                //    options.UsePersistentMessagesByDefault = false;
                //})
                //.UseRedis(options =>
                //{
                //    options.EndPoints.Add("localhost:6379");
                //})
                ;

            if (_batchSize.HasValue)
            {
                builder.WithBatchSize(_batchSize.Value);
            }

            foreach (var partition in _partitions ?? Array.Empty<PartitionInfo>())
            {
                if (string.IsNullOrEmpty(partition.Table) || partition.Columns == null || partition.Columns.Length == 0)
                {
                    throw new Exception("Invalid partition definition - missing table name or columns");
                }

                if (partition.PartitionCount.HasValue)
                {
                    builder.WithPgKeyColumn(partition.Table, partition.PartitionCount.Value, partition.Columns);
                }
                else
                {
                    builder.WithPgKeyColumn(partition.Table, partition.Columns);
                }
            }

            if (_filterFrom.HasValue && _filterTo.HasValue)
            {
                builder.WithPartitionFilter(_filterFrom.Value, _filterTo.Value);
            }

            using var pgOutput2Json = builder.Build();

            await pgOutput2Json.Start(stoppingToken);
        }
    }
}