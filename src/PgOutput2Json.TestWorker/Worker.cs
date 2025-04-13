using PgOutput2Json.RabbitMqStreams;
using System.Net;

namespace PgOutput2Json.TestWorker
{
    /// <summary>
    /// For this code to work it is expected that:
    /// - the PostgreSQL cluster is running with wal_level = logical
    /// - the connection string in the appsettings.json points to an existing PostgreSQL database, with the correct username and password
    /// - a publication is created for the tables that are to be tracked, and the name of the publication matches the one in the appsettings.json
    /// 
    /// For more detailed information please refer to the readme file at https://github.com/PgOutput2Json/PgOutput2Json
    /// </summary>
    public class Worker : BackgroundService
    {
        private class PartitionInfo
        {
            public string? Table { get; set; }
            public int? PartitionCount { get; set; }
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
                    Console.WriteLine($"{table} ({key}): {json}");
                    return Task.FromResult(true);
                })
                //.UseRabbitMq(options =>
                //{
                //    options.HostNames.Add("localhost");
                //    options.ConnectionFactory.UserName = "guest";
                //    options.ConnectionFactory.Password = "guest";
                //    options.ConnectionFactory.VirtualHost = "/";
                //    options.ExchangeName = "my_exchange";
                //    options.UsePersistentMessagesByDefault = false;
                //})
                //.UseRedis(options =>
                //{
                //    options.Redis.EndPoints.Add("localhost:6379");
                //})
                //.UseRabbitMqStreams(options =>
                //{
                //    options.StreamName = "my_stream";
                //    options.UseDeduplication = true;

                //    options.StreamSystemConfig.UserName = "guest";
                //    options.StreamSystemConfig.Password = "guest";
                //    options.StreamSystemConfig.VirtualHost = "/";
                //    options.StreamSystemConfig.Endpoints =
                //    [
                //        new IPEndPoint(IPAddress.Loopback, 5552)
                //    ];
                //})
                ;

            if (_batchSize.HasValue)
            {
                builder = builder.WithBatchSize(_batchSize.Value);
            }

            foreach (var partition in _partitions ?? Array.Empty<PartitionInfo>())
            {
                if (string.IsNullOrEmpty(partition.Table))
                {
                    throw new Exception("Invalid partition definition - missing table name");
                }

                builder.WithTablePartitions(partition.Table, partition.PartitionCount ?? 1);
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