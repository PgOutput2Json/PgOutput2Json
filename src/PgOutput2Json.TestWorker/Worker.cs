using Amazon;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;
using MongoDB.Driver;
using MongoDB.Driver.Core.Configuration;
using System.Net;
using System.Text.Json;

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

        public Worker(ILoggerFactory loggerFactory, IConfiguration configuration)
        {
            _loggerFactory = loggerFactory;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var pgOutput2Json = PgOutput2JsonBuilder.Create()
                .WithLoggerFactory(_loggerFactory)
                .WithPgConnectionString("Host=localhost;Database=test_db;Username=postgres;Password=postgres")
                .WithPgPublications("test_publication")
                .WithPgReplicationSlot("test_slot")
                //.WithBatchSize(500) // default is 100
                //.WithPgColumns("public.test_table", "id", "first_name") // optional, use it to filter the columns written to JSON
                //.WithPgOrderedKeyColumns("public.test_table", "id") // used for initial data copy, to support ordering and resuming an interrupted initial data copy process
                .WithInitialDataCopy(true) // pushes the existing data to the publisher - a separate schema named pgoutput2json must be created in the source db (to track the progress)
                .WithJsonOptions(options =>
                {
                    //options.WriteNulls = true;
                    //options.WriteTimestamps = true;
                    //options.WriteTableNames = true;
                    //options.WriteMode = JsonWriteMode.Compact;
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
                //    options.ExchangeName = "amq.topic";
                //    options.UsePersistentMessagesByDefault = false;
                //})
                //.UseRedis(options =>
                //{
                //    options.StreamName = "test_stream";
                //    options.Redis.EndPoints.Add("localhost:6379");
                //})
                //.UseRabbitMqStreams(options =>
                //{
                //    options.StreamName = "test_stream";
                //    options.StreamSystemConfig.UserName = "guest";
                //    options.StreamSystemConfig.Password = "guest";
                //    options.StreamSystemConfig.VirtualHost = "/";
                //    options.StreamSystemConfig.Endpoints =
                //    [
                //        new IPEndPoint(IPAddress.Loopback, 5552)
                //    ];
                //})
                //.UseKafka(options =>
                //{
                //    options.ProducerConfig.BootstrapServers = "localhost:9092";
                //    options.Topic = "test_topic";
                //})
                //.UseSqlite(options =>
                //{
                //    options.ConnectionStringBuilder.DataSource = "test_database.s3db";
                //})
                //.UseMongoDb(options =>
                //{
                //    options.DatabaseName = "test_database";
                //    options.ClientSettings = new MongoClientSettings
                //    {
                //        Server = new MongoServerAddress("localhost", 27017),
                //        Scheme = ConnectionStringScheme.MongoDB,
                //        Credential = MongoCredential.CreateCredential("admin", "admin", "secret"),
                //    };
                //})
                //.UseKinesis(options =>
                //{
                //    // assumes LocalStack, with created test_stream in eu-central-1
                //    options.StreamName = "test_stream";
                //    options.KinesisConfig.ServiceURL = "http://localhost:4566";
                //    options.KinesisConfig.UseHttp = true;
                //    options.KinesisConfig.DefaultAWSCredentials = new BasicAWSCredentials("dummy", "dummy");
                //    options.KinesisConfig.AuthenticationRegion = "eu-central-1";
                //})
                .Build();

            await pgOutput2Json.StartAsync(stoppingToken);
        }
    }
}