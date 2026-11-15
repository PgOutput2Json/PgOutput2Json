using System.Globalization;

namespace PgOutput2Json.Benchmark
{
    internal sealed class BenchmarkOptions
    {
        public const string Usage = """
            Measures how fast PgOutput2Json copies and streams rows from PostgreSQL to an output.

            dotnet run -c Release --project src/PgOutput2Json.Benchmark -- [options]

              --scenario copy,stream     The initial copy of a table, and catching up with rows already in the WAL (default: both)
              --output null,clickhouse,sqlite,rabbitmqstreams
                                         null: only counts the rows, which measures the library itself (default: null)
              --rows N                   Rows per scenario (default: 1000000)
              --workers N                Copy workers per table (default: 1)
              --tx-rows N                Rows per transaction of the stream scenario (default: 1000)
              --runs N                   Runs of each scenario: the table has the fastest and the median (default: 1)
              --batch-size N             Batch size of the listener (default: that of the output)
              --compact                  The compact write mode for the null output
              --timeout SECONDS          Gives up after this long (default: 1800)
              --pg CONNECTION_STRING     A PostgreSQL with wal_level=logical, instead of a container
              --pg-image IMAGE           The image of the container (default: postgres:16)
              --clickhouse URL           A ClickHouse HTTP endpoint, instead of a container
              --clickhouse-user USER     (default: default)
              --clickhouse-password PWD  (default: bench)
              --clickhouse-database DB   Dropped and created again for every run (default: pg2j_bench)
              --clickhouse-format F      json or rowbinary: the format of the inserts (default: json, as the output)
              --sqlite FILE              The SQLite database, deleted before every run (default: pg2j_bench.db in the temp directory)
              --sqlite-wal               The WAL journal mode for SQLite (UseWal)
              --rabbitmq HOST:PORT       A RabbitMQ stream endpoint, instead of a container (stream pg2j_bench, created again for every run)
              --rabbitmq-user USER       (default: rabbitmq)
              --rabbitmq-password PWD    (default: rabbitmq)
              --rabbitmq-buffer N        Messages per frame (MessageBufferSize, default: that of the output)
            """;

        public string[] Scenarios { get; private set; } = ["copy", "stream"];
        public string[] Outputs { get; private set; } = ["null"];
        public long Rows { get; private set; } = 1_000_000;
        public int Workers { get; private set; } = 1;
        public long TxRows { get; private set; } = 1000;
        public int Runs { get; private set; } = 1;
        public int? BatchSize { get; private set; }
        public bool Compact { get; private set; }
        public TimeSpan Timeout { get; private set; } = TimeSpan.FromMinutes(30);
        public string? PgConnectionString { get; private set; }
        public string PgImage { get; private set; } = "postgres:16";
        public string? ClickHouseEndpoint { get; private set; }
        public string ClickHouseUsername { get; private set; } = "default";
        public string ClickHousePassword { get; private set; } = "bench";
        public string ClickHouseDatabase { get; private set; } = "pg2j_bench";
        public bool ClickHouseRowBinary { get; private set; }
        public string SqliteFile { get; private set; } = Path.Combine(Path.GetTempPath(), "pg2j_bench.db");
        public bool SqliteWal { get; private set; }
        public string? RabbitMqEndpoint { get; private set; }
        public string RabbitMqUsername { get; private set; } = "rabbitmq";
        public string RabbitMqPassword { get; private set; } = "rabbitmq";
        public int? RabbitMqBufferSize { get; private set; }

        public static BenchmarkOptions? Parse(string[] args)
        {
            var options = new BenchmarkOptions();

            for (var i = 0; i < args.Length; i++)
            {
                string Value() => i + 1 < args.Length ? args[++i] : throw new ArgumentException($"{args[i]} needs a value");

                long Number() => long.Parse(Value(), CultureInfo.InvariantCulture);

                string[] List() => Value().Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

                switch (args[i])
                {
                    case "--scenario": options.Scenarios = List(); break;
                    case "--output": options.Outputs = List(); break;
                    case "--rows": options.Rows = Number(); break;
                    case "--workers": options.Workers = (int)Number(); break;
                    case "--tx-rows": options.TxRows = Number(); break;
                    case "--runs": options.Runs = (int)Number(); break;
                    case "--batch-size": options.BatchSize = (int)Number(); break;
                    case "--compact": options.Compact = true; break;
                    case "--timeout": options.Timeout = TimeSpan.FromSeconds(Number()); break;
                    case "--pg": options.PgConnectionString = Value(); break;
                    case "--pg-image": options.PgImage = Value(); break;
                    case "--clickhouse": options.ClickHouseEndpoint = Value(); break;
                    case "--clickhouse-user": options.ClickHouseUsername = Value(); break;
                    case "--clickhouse-password": options.ClickHousePassword = Value(); break;
                    case "--clickhouse-database": options.ClickHouseDatabase = Value(); break;
                    case "--sqlite": options.SqliteFile = Value(); break;
                    case "--sqlite-wal": options.SqliteWal = true; break;
                    case "--rabbitmq": options.RabbitMqEndpoint = Value(); break;
                    case "--rabbitmq-user": options.RabbitMqUsername = Value(); break;
                    case "--rabbitmq-password": options.RabbitMqPassword = Value(); break;
                    case "--rabbitmq-buffer": options.RabbitMqBufferSize = (int)Number(); break;
                    case "--clickhouse-format":
                        var format = Value();
                        if (format is not ("rowbinary" or "json")) return null;
                        options.ClickHouseRowBinary = format == "rowbinary";
                        break;
                    default: return null;
                }
            }

            if (options.Scenarios.Any(s => s is not ("copy" or "stream"))) return null;
            if (options.Outputs.Any(o => o is not ("null" or "clickhouse" or "sqlite" or "rabbitmqstreams"))) return null;

            return options.Rows > 0 && options.Workers > 0 && options.TxRows > 0 && options.Runs > 0 ? options : null;
        }
    }
}
