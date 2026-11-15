using System.Net;
using System.Diagnostics;
using System.Globalization;

using Microsoft.Extensions.Logging;

using Npgsql;

using PgOutput2Json;
using PgOutput2Json.Benchmark;
using PgOutput2Json.ClickHouse;

using Testcontainers.ClickHouse;
using Testcontainers.PostgreSql;
using Testcontainers.RabbitMq;

using RabbitMQ.Stream.Client;

var options = BenchmarkOptions.Parse(args);

if (options == null)
{
    Console.WriteLine(BenchmarkOptions.Usage);
    return 1;
}

using var loggerFactory = LoggerFactory.Create(b => b.AddSimpleConsole(o => o.SingleLine = true).SetMinimumLevel(LogLevel.Warning));

PostgreSqlContainer? postgres = null;
ClickHouseContainer? clickHouse = null;
RabbitMqContainer? rabbitMq = null;

try
{
    var pgConnectionString = options.PgConnectionString;

    if (pgConnectionString == null)
    {
        Console.WriteLine($"Starting {options.PgImage}...");

        // a little more room than the defaults, so that loading the rows is not what the benchmark measures
        postgres = new PostgreSqlBuilder(options.PgImage)
            .WithCommand("-c", "wal_level=logical", "-c", "shared_buffers=512MB", "-c", "max_wal_size=8GB", "-c", "max_replication_slots=20", "-c", "max_wal_senders=20")
            .Build();

        await postgres.StartAsync();

        // on Linux the container is reached at its own address, not through the port that Docker maps
        // with its proxy, which costs throughput; Docker Desktop (macOS, Windows) has no route to it
        pgConnectionString = OperatingSystem.IsLinux()
            ? new NpgsqlConnectionStringBuilder(postgres.GetConnectionString()) { Host = postgres.IpAddress, Port = 5432 }.ConnectionString
            : postgres.GetConnectionString();
    }

    var clickHouseEndpoint = options.ClickHouseEndpoint;

    if (options.Outputs.Contains("clickhouse") && clickHouseEndpoint == null)
    {
        Console.WriteLine("Starting clickhouse/clickhouse-server...");

        clickHouse = new ClickHouseBuilder("clickhouse/clickhouse-server:latest")
            .WithUsername(options.ClickHouseUsername)
            .WithPassword(options.ClickHousePassword)
            .Build();

        await clickHouse.StartAsync();

        clickHouseEndpoint = OperatingSystem.IsLinux()
            ? $"http://{clickHouse.IpAddress}:8123"
            : $"http://{clickHouse.Hostname}:{clickHouse.GetMappedPublicPort(8123)}";
    }

    IPEndPoint? rabbitMqEndpoint = options.RabbitMqEndpoint != null ? IPEndPoint.Parse(options.RabbitMqEndpoint) : null;

    if (options.Outputs.Contains("rabbitmqstreams") && rabbitMqEndpoint == null)
    {
        Console.WriteLine("Starting rabbitmq:4-management...");

        rabbitMq = new RabbitMqBuilder("rabbitmq:4-management").WithPortBinding(5552, true).Build();

        await rabbitMq.StartAsync();

        var plugin = await rabbitMq.ExecAsync(["rabbitmq-plugins", "enable", "rabbitmq_stream"]);

        if (plugin.ExitCode != 0) throw new Exception($"Could not enable the stream plugin: {plugin.Stderr}");

        rabbitMqEndpoint = OperatingSystem.IsLinux()
            ? new IPEndPoint(IPAddress.Parse(rabbitMq.IpAddress), 5552)
            : new IPEndPoint(IPAddress.Loopback, rabbitMq.GetMappedPublicPort(5552));
    }

    // the server advertises its own host name to the clients, which the resolver replaces with the endpoint
    StreamSystemConfig RabbitMqConfig() => new()
    {
        UserName = options.RabbitMqUsername,
        Password = options.RabbitMqPassword,
        Endpoints = [rabbitMqEndpoint!],
        AddressResolver = new AddressResolver(rabbitMqEndpoint!),
    };

    var results = new List<(Result Best, Result Median)>();

    foreach (var scenario in options.Scenarios)
    {
        foreach (var output in options.Outputs)
        {
            var runs = new List<Result>();

            for (var run = 1; run <= options.Runs; run++)
            {
                Console.WriteLine($"Running {scenario} with {output}, {options.Rows:N0} rows{(options.Runs > 1 ? $", run {run} of {options.Runs}" : "")}...");

                runs.Add(await RunAsync(scenario, output));
            }

            runs.Sort((a, b) => a.Elapsed.CompareTo(b.Elapsed));

            results.Add((runs[0], runs[runs.Count / 2]));
        }
    }

    Console.WriteLine();
    Console.WriteLine($"{options.Rows:N0} rows, {options.Workers} copy worker(s) per table, {(options.Runs > 1 ? $"fastest of {options.Runs} runs (median in brackets), " : "")} PostgreSQL {await ServerVersionAsync(pgConnectionString)}, .NET {Environment.Version}, {Environment.ProcessorCount} CPUs");
    Console.WriteLine();
    Console.WriteLine("| Scenario | Output | Seconds | Rows/s | CPU seconds | Allocated MB | Bytes/row | GC 0/1/2 |");
    Console.WriteLine("|---|---|---:|---:|---:|---:|---:|---|");

    foreach (var (r, median) in results)
    {
        var m = options.Runs > 1 ? string.Create(CultureInfo.InvariantCulture, $" ({median.Elapsed.TotalSeconds:F1})") : "";
        var mRate = options.Runs > 1 ? string.Create(CultureInfo.InvariantCulture, $" ({options.Rows / median.Elapsed.TotalSeconds:N0})") : "";

        Console.WriteLine(string.Create(CultureInfo.InvariantCulture,
            $"| {r.Scenario} | {r.Output} | {r.Elapsed.TotalSeconds:F1}{m} | {options.Rows / r.Elapsed.TotalSeconds:N0}{mRate} | {r.Cpu.TotalSeconds:F1} | {r.AllocatedBytes / 1048576.0:N0} | {r.AllocatedBytes / (double)options.Rows:N0} | {r.Gc0}/{r.Gc1}/{r.Gc2} |"));
    }

    return 0;

    async Task<Result> RunAsync(string scenario, string output)
    {
        var table = $"pg2j_bench_{scenario}";
        var slot = $"{table}_slot";

        await PrepareAsync(pgConnectionString, table, slot, scenario, options);

        if (output == "clickhouse") await ClickHouseAsync(clickHouseEndpoint!, options, $"DROP DATABASE IF EXISTS {options.ClickHouseDatabase} SYNC; CREATE DATABASE {options.ClickHouseDatabase}");

        var builder = PgOutput2JsonBuilder.Create()
            .WithLoggerFactory(loggerFactory)
            .WithPgConnectionString(pgConnectionString)
            .WithPgPublications($"{table}_pub")
            .WithPgReplicationSlot(slot)
            .WithInitialDataCopy(scenario == "copy", 1, options.Workers);

        if (output == "clickhouse")
        {
            builder.UseClickHouse(o =>
            {
                o.Endpoint = clickHouseEndpoint!;
                o.Database = options.ClickHouseDatabase;
                o.Username = options.ClickHouseUsername;
                o.Password = options.ClickHousePassword;
                o.UseRowBinary = options.ClickHouseRowBinary;
            });
        }
        else if (output == "rabbitmqstreams")
        {
            await RecreateStreamAsync(RabbitMqConfig());

            builder.UseRabbitMqStreams(o =>
            {
                o.StreamName = BenchmarkStream;
                o.StreamSystemConfig = RabbitMqConfig();

                if (options.RabbitMqBufferSize != null) o.MessageBufferSize = options.RabbitMqBufferSize.Value;
            });
        }
        else if (output == "sqlite")
        {
            foreach (var suffix in new[] { "", "-wal", "-shm", "-journal" }) File.Delete(options.SqliteFile + suffix);

            builder.UseSqlite(o =>
            {
                o.ConnectionStringBuilder.DataSource = options.SqliteFile;
                o.UseWal = options.SqliteWal;
            });
        }
        else if (options.Compact)
        {
            builder.WithJsonOptions(o => o.WriteMode = JsonWriteMode.Compact);
        }

        if (options.BatchSize != null) builder.WithBatchSize(options.BatchSize.Value);

        var counter = new CountingPublisherFactory(builder.MessagePublisherFactory, $"public.{table}", options.Rows);

        builder.WithMessagePublisherFactory(counter);

        // what happened before (loading the rows, the containers) is not measured
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        var process = Process.GetCurrentProcess();

        var cpu = process.TotalProcessorTime;
        var allocated = GC.GetTotalAllocatedBytes(true);
        var (gc0, gc1, gc2) = (GC.CollectionCount(0), GC.CollectionCount(1), GC.CollectionCount(2));

        var stopwatch = Stopwatch.StartNew();

        using var cts = new CancellationTokenSource();

        using var pgOutput2Json = builder.Build();

        var listener = pgOutput2Json.StartAsync(cts.Token);

        var finished = await Task.WhenAny(counter.Done.Task, listener, Task.Delay(options.Timeout));

        stopwatch.Stop();

        process.Refresh();

        var result = new Result(scenario,
                                output,
                                stopwatch.Elapsed,
                                process.TotalProcessorTime - cpu,
                                GC.GetTotalAllocatedBytes(true) - allocated,
                                GC.CollectionCount(0) - gc0,
                                GC.CollectionCount(1) - gc1,
                                GC.CollectionCount(2) - gc2);

        await cts.CancelAsync();

        try
        {
            await listener;
        }
        catch (OperationCanceledException)
        {
        }

        if (finished != counter.Done.Task)
        {
            throw new Exception($"{scenario} with {output}: {counter.Confirmed:N0} of {options.Rows:N0} rows confirmed after {stopwatch.Elapsed.TotalSeconds:F0} seconds");
        }

        return result;
    }
}
finally
{
    if (postgres != null) await postgres.DisposeAsync();
    if (clickHouse != null) await clickHouse.DisposeAsync();
    if (rabbitMq != null) await rabbitMq.DisposeAsync();
}

/// <summary>
/// A fresh table, publication and slot; a table created again is a new table for the listener, so it is copied again. For the copy the rows are in the table before the listener starts.
/// For the stream they are inserted after the slot is created, in transactions of TxRows rows, so the
/// listener catches up with all of them from the WAL.
/// </summary>
static async Task PrepareAsync(string connectionString, string table, string slot, string scenario, BenchmarkOptions options)
{
    await using var connection = new NpgsqlConnection(connectionString);
    await connection.OpenAsync();

    async Task ExecuteAsync(string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 0 };
        await command.ExecuteNonQueryAsync();
    }

    await ExecuteAsync($@"
        SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '{slot}' OR slot_name LIKE '{slot}\_sync\_%';
        DROP PUBLICATION IF EXISTS {table}_pub;
        DROP TABLE IF EXISTS {table};
        CREATE TABLE {table} (
            id bigint PRIMARY KEY,
            account int NOT NULL,
            name text NOT NULL,
            amount numeric(12,2),
            created_at timestamptz NOT NULL,
            active bool NOT NULL,
            tags text[],
            payload jsonb);
        CREATE PUBLICATION {table}_pub FOR TABLE {table};");

    var values = $@"SELECT i, i % 1000, 'customer ' || i, (i % 100000) / 100.0, timestamptz '2025-01-01' + i * interval '1 second', i % 2 = 0,
                           ARRAY['tag' || (i % 10), 'tag' || (i % 7)], jsonb_build_object('n', i, 'kind', 'k' || (i % 5))";

    if (scenario == "copy")
    {
        await ExecuteAsync($"INSERT INTO {table} {values} FROM generate_series(1, {options.Rows}) i");

        // a VACUUM cannot run in a batch of statements
        await ExecuteAsync($"VACUUM ANALYZE {table}");
        await ExecuteAsync($"SELECT pg_create_logical_replication_slot('{slot}', 'pgoutput')");
    }
    else
    {
        await ExecuteAsync($"SELECT pg_create_logical_replication_slot('{slot}', 'pgoutput')");

        for (var start = 1L; start <= options.Rows; start += options.TxRows)
        {
            var end = Math.Min(start + options.TxRows - 1, options.Rows);

            await ExecuteAsync($"INSERT INTO {table} {values} FROM generate_series({start}, {end}) i");
        }
    }
}

/// <summary>
/// A new, empty stream; the stream plugin may still be starting.
/// </summary>
static async Task RecreateStreamAsync(StreamSystemConfig config)
{
    for (var attempt = 1; ; attempt++)
    {
        try
        {
            var system = await StreamSystem.Create(config);

            try
            {
                if (await system.StreamExists(BenchmarkStream)) await system.DeleteStream(BenchmarkStream);

                await system.CreateStream(new StreamSpec(BenchmarkStream));
                return;
            }
            finally
            {
                await system.Close();
            }
        }
        catch (Exception) when (attempt < 30)
        {
            await Task.Delay(1000);
        }
    }
}

static async Task ClickHouseAsync(string endpoint, BenchmarkOptions options, string sql)
{
    using var http = new HttpClient();

    foreach (var statement in sql.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, endpoint) { Content = new StringContent(statement) };

        request.Headers.Add("X-ClickHouse-User", options.ClickHouseUsername);
        request.Headers.Add("X-ClickHouse-Key", options.ClickHousePassword);

        using var response = await http.SendAsync(request);

        if (!response.IsSuccessStatusCode) throw new Exception($"ClickHouse: {await response.Content.ReadAsStringAsync()}");
    }
}

static async Task<string> ServerVersionAsync(string connectionString)
{
    await using var connection = new NpgsqlConnection(connectionString);
    await connection.OpenAsync();

    return connection.ServerVersion;
}

internal sealed partial class Program
{
    private const string BenchmarkStream = "pg2j_bench";
}

internal sealed record Result(string Scenario, string Output, TimeSpan Elapsed, TimeSpan Cpu, long AllocatedBytes, int Gc0, int Gc1, int Gc2);
