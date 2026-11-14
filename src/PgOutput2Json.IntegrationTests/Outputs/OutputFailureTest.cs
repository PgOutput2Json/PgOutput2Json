using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Microsoft.Data.Sqlite;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MongoDB.Bson;
using MongoDB.Driver;
using RabbitMQ.Client;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using StackExchange.Redis;

using Consumer = RabbitMQ.Stream.Client.Reliable.Consumer;

namespace PgOutput2Json.IntegrationTests.Outputs
{
    /// <summary>
    /// Every output with a failure it must report: a missing destination, or the server going down.
    /// </summary>
    [TestClass]
    public class OutputFailureTest
    {
        [TestMethod]
        public async Task RabbitMq_without_the_exchange_should_not_confirm_and_deliver_once_it_exists()
        {
            var container = await Containers.GetRabbitMqAsync();

            var factory = new ConnectionFactory { Uri = new Uri(container.GetConnectionString()) };

            await using var connection = await factory.CreateConnectionAsync();
            await using var channel = await connection.CreateChannelAsync();

            // the queue exists, so nothing published after the repair is lost
            await channel.QueueDeclareAsync("of_rabbit_q", durable: true, exclusive: false, autoDelete: false);

            var bodies = new List<string>();

            await FailureScenario.RunAsync("of_rabbit",
                builder => builder.UseRabbitMq(options =>
                {
                    options.ExchangeName = "of_rabbit";
                    options.ConnectionFactory = factory;
                }),
                breakAsync: () => Task.FromResult<Func<Task>>(async () =>
                {
                    await channel.ExchangeDeclareAsync("of_rabbit", ExchangeType.Topic, durable: true);
                    await channel.QueueBindAsync("of_rabbit_q", "of_rabbit", "#");
                }),
                readIdsAsync: async () =>
                {
                    while (await channel.BasicGetAsync("of_rabbit_q", autoAck: true) is { } result)
                    {
                        bodies.Add(Encoding.UTF8.GetString(result.Body.Span));
                    }

                    return FailureScenario.Ids(bodies);
                },
                breakBeforeStart: true);
        }

        [TestMethod]
        public async Task RabbitMq_stream_that_does_not_exist_should_not_confirm_and_deliver_once_it_exists()
        {
            await Containers.GetRabbitMqAsync();

            var endpoint = new IPEndPoint(IPAddress.Loopback, Containers.RabbitMqStreamPort);

            StreamSystemConfig CreateConfig() => new()
            {
                UserName = "rabbitmq",
                Password = "rabbitmq",
                Endpoints = [endpoint],
                AddressResolver = new AddressResolver(endpoint),
            };

            var system = await StreamSystem.Create(CreateConfig());

            var bodies = new List<string>();
            Consumer? consumer = null;

            try
            {
                await FailureScenario.RunAsync("of_stream",
                    builder => builder.UseRabbitMqStreams(options =>
                    {
                        options.StreamName = "of_stream";
                        options.StreamSystemConfig = CreateConfig();
                    }),
                    breakAsync: () => Task.FromResult<Func<Task>>(async () =>
                    {
                        await system.CreateStream(new StreamSpec("of_stream"));

                        consumer = await Consumer.Create(new ConsumerConfig(system, "of_stream")
                        {
                            OffsetSpec = new OffsetTypeFirst(),
                            MessageHandler = (_, _, _, message) =>
                            {
                                lock (bodies) bodies.Add(Encoding.UTF8.GetString(message.Data.Contents));

                                return Task.CompletedTask;
                            },
                        });
                    }),
                    readIdsAsync: () =>
                    {
                        lock (bodies) return Task.FromResult(FailureScenario.Ids(bodies));
                    },
                    breakBeforeStart: true);
            }
            finally
            {
                if (consumer != null) await consumer.Close();

                await system.Close();
            }
        }

        [TestMethod]
        public async Task Kinesis_stream_that_does_not_exist_should_not_confirm_and_deliver_once_it_exists()
        {
            var config = new AmazonKinesisConfig { ServiceURL = await Containers.GetLocalStackUrlAsync(), AuthenticationRegion = "us-east-1" };

            using var client = new AmazonKinesisClient(config);

            await FailureScenario.RunAsync("of_kinesis",
                builder => builder.UseKinesis(options =>
                {
                    options.StreamName = "of_kinesis";
                    options.KinesisConfig = config;
                }),
                breakAsync: () => Task.FromResult<Func<Task>>(async () =>
                {
                    await client.CreateStreamAsync(new CreateStreamRequest { StreamName = "of_kinesis", ShardCount = 1 });
                }),
                readIdsAsync: async () =>
                {
                    try
                    {
                        var shardId = (await client.ListShardsAsync(new ListShardsRequest { StreamName = "of_kinesis" })).Shards[0].ShardId;

                        var iterator = (await client.GetShardIteratorAsync(new GetShardIteratorRequest
                        {
                            StreamName = "of_kinesis",
                            ShardId = shardId,
                            ShardIteratorType = ShardIteratorType.TRIM_HORIZON,
                        })).ShardIterator;

                        var records = (await client.GetRecordsAsync(new GetRecordsRequest { ShardIterator = iterator })).Records;

                        return FailureScenario.Ids(records.Select(r => Encoding.UTF8.GetString(r.Data.ToArray())));
                    }
                    catch (AmazonKinesisException)
                    {
                        return []; // no stream yet, or not active yet
                    }
                },
                breakBeforeStart: true);
        }

        [TestMethod]
        public async Task Redis_that_goes_down_should_not_confirm_and_deliver_once_it_is_back()
        {
            var connectionString = await Containers.GetRedisConnectionStringAsync();

            await FailureScenario.RunAsync("of_redis",
                builder => builder.UseRedis(options =>
                {
                    options.StreamName = "of_redis";
                    options.Redis = ConfigurationOptions.Parse(connectionString);
                }),
                breakAsync: () => Containers.StopAsync(Containers.Redis),
                readIdsAsync: async () =>
                {
                    try
                    {
                        using var redis = await ConnectionMultiplexer.ConnectAsync(connectionString);

                        return FailureScenario.Ids((await redis.GetDatabase().StreamRangeAsync("of_redis")).Select(e => e["m"].ToString()));
                    }
                    catch (RedisException)
                    {
                        return [];
                    }
                });
        }

        [TestMethod]
        public async Task MongoDb_that_goes_down_should_not_confirm_and_deliver_once_it_is_back()
        {
            // its own server: the MongoDB module of Testcontainers cannot start a container a second time
            var port = Containers.FreePort();

            await using var container = new DotNet.Testcontainers.Builders.ContainerBuilder("mongo:8")
                .WithPortBinding(port, 27017)
                .WithWaitStrategy(DotNet.Testcontainers.Builders.Wait.ForUnixContainer().UntilMessageIsLogged("Waiting for connections"))
                .Build();

            await container.StartAsync();

            var settings = MongoClientSettings.FromConnectionString($"mongodb://127.0.0.1:{port}/?directConnection=true");

            settings.ServerSelectionTimeout = TimeSpan.FromSeconds(3);

            using var client = new MongoClient(settings);

            await FailureScenario.RunAsync("of_mongo",
                builder => builder.UseMongoDb(options =>
                {
                    options.ClientSettings = settings;
                    options.DatabaseName = "of_mongo_db";
                }),
                breakAsync: () => Containers.StopAsync(container),
                readIdsAsync: async () =>
                {
                    try
                    {
                        var documents = await client.GetDatabase("of_mongo_db").GetCollection<BsonDocument>("public__of_mongo")
                            .Find(FilterDefinition<BsonDocument>.Empty).ToListAsync();

                        return documents.Select(d => d["id"].ToInt32()).ToList();
                    }
                    catch (Exception ex) when (ex is MongoException or TimeoutException)
                    {
                        return [];
                    }
                });
        }

        [TestMethod]
        public async Task DynamoDb_that_goes_down_should_not_confirm_and_deliver_once_it_is_back()
        {
            var config = new AmazonDynamoDBConfig { ServiceURL = await Containers.GetDynamoDbUrlAsync(), MaxErrorRetry = 0, Timeout = TimeSpan.FromSeconds(3) };

            using var client = new AmazonDynamoDBClient(config);

            await FailureScenario.RunAsync("of_dynamo",
                builder => builder.UseDynamoDb(options => options.ClientConfig = config),
                breakAsync: () => Containers.StopAsync(Containers.DynamoDb),
                readIdsAsync: async () =>
                {
                    try
                    {
                        var items = (await client.ScanAsync(new ScanRequest { TableName = "public.of_dynamo", ConsistentRead = true })).Items;

                        return items.Select(item => int.Parse(item["id"].N)).ToList();
                    }
                    catch (Exception ex) when (ex is AmazonDynamoDBException or System.Net.Http.HttpRequestException or Amazon.Runtime.AmazonServiceException or OperationCanceledException)
                    {
                        return [];
                    }
                });
        }

        [TestMethod]
        public async Task Sqlite_file_locked_by_another_writer_should_not_confirm_and_deliver_once_it_is_free()
        {
            var path = Path.Combine(Path.GetTempPath(), $"pg2j_{Guid.NewGuid():N}.db");

            // the publisher gives up on a locked file after one second instead of thirty
            var connectionString = $"Data Source={path};Pooling=False;Default Timeout=1";

            try
            {
                await FailureScenario.RunAsync("of_sqlite",
                    builder => builder.UseSqlite(options => options.ConnectionStringBuilder = new SqliteConnectionStringBuilder(connectionString)),
                    breakAsync: async () =>
                    {
                        var blocker = new SqliteConnection(connectionString);

                        await blocker.OpenAsync();

                        await using (var command = blocker.CreateCommand())
                        {
                            command.CommandText = "BEGIN EXCLUSIVE";
                            await command.ExecuteNonQueryAsync();
                        }

                        return async () => await blocker.DisposeAsync();
                    },
                    readIdsAsync: async () =>
                    {
                        try
                        {
                            await using var connection = new SqliteConnection(connectionString);
                            await connection.OpenAsync();

                            await using var command = connection.CreateCommand();

                            command.CommandText = "SELECT id FROM of_sqlite ORDER BY id";

                            var ids = new List<int>();

                            await using var reader = await command.ExecuteReaderAsync();

                            while (await reader.ReadAsync()) ids.Add(reader.GetInt32(0));

                            return ids;
                        }
                        catch (SqliteException)
                        {
                            return []; // no table yet, or locked
                        }
                    });
            }
            finally
            {
                File.Delete(path);
            }
        }
    }
}
