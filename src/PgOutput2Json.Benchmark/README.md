# PgOutput2Json.Benchmark

Measures how fast PgOutput2Json copies and streams rows from PostgreSQL to an output, end to end, on your machine.

```
dotnet run -c Release --project src/PgOutput2Json.Benchmark -- --rows 1000000 --output null,clickhouse,sqlite,mongodb,dynamodb,rabbitmq,rabbitmqstreams,kafka,webhooks
```

A shared or virtual machine gives noisy times: `--runs 3` runs each scenario three times and reports the fastest and the median. To compare two versions, run them one after the other, more than once.

It needs Docker: it starts PostgreSQL (and ClickHouse, for that output) with Testcontainers, unless you give it your own with `--pg` and `--clickhouse`. On Linux it connects to the containers at their own addresses, because the port that Docker maps goes through a proxy that costs throughput. Run `dotnet run -c Release --project src/PgOutput2Json.Benchmark -- --help` for all the options.

## Scenarios

Each scenario gets a fresh table, publication and replication slot, with rows of eight columns: `bigint`, `int`, `text`, `numeric(12,2)`, `timestamptz`, `bool`, `text[]` and `jsonb`.

- **copy**: the rows are in the table before the listener starts, and are published by the initial copy (`--workers` sets the copy workers per table).
- **stream**: the rows are inserted after the slot is created, in transactions of `--tx-rows` rows, while the listener is not running. Then the listener starts and catches up with all of them from the WAL. Loading the rows is not measured, only the replication.

## Outputs

- **null**: counts the rows and does nothing else. It measures the library itself: reading the replication stream or the copy, and writing the JSON.
- **clickhouse**: the ClickHouse output, into a database that is dropped and created again for every run.
- **sqlite**: the SQLite output, into a file that is deleted before every run (`--sqlite`, `--sqlite-wal`).
- **mongodb**: the MongoDB output, into a database that is dropped before every run (`--mongodb`).
- **dynamodb**: the DynamoDB output, into a DynamoDB Local container whose tables are all deleted before every run (`--dynamodb URL` for another endpoint, `--dynamodb-parallel`). DynamoDB Local is not DynamoDB: its numbers say little about AWS.
- **rabbitmq**: the RabbitMQ output, into a topic exchange with a durable queue bound to it, both created again for every run (`--rabbitmq-amqp`, `--rabbitmq-transient`).
- **rabbitmqstreams**: the RabbitMQ Streams output, into a stream that is created again for every run (`--rabbitmq`, `--rabbitmq-buffer`).
- **webhooks**: the webhooks output, to a receiver in the process of the benchmark that reads every batch and answers 200 (`--webhook URL` for another one, `--webhook-secret`).
- **kafka**: the Kafka output, into a topic that is created again for every run (`--kafka`, `--kafka-partitions`, `--kafka-config linger.ms=20`). The Kafka container advertises its own address, so the clients do not go through Docker's proxy.

The time ends when the output has confirmed every row. A wrapper around the output's publishers counts them, so it is measured the same way for every output, and nothing polls the target.

## Results

The table lists, for each scenario and output: the time, the rows per second, the CPU time of the process (all threads), the bytes allocated (in total and per row), and the garbage collections. The CPU time and the allocations are those of PgOutput2Json and the output, not of PostgreSQL or ClickHouse.

## Profiling

Build it once, then run the benchmark under [dotnet-trace](https://learn.microsoft.com/dotnet/core/diagnostics/dotnet-trace), for example for one scenario and output:

```
dotnet tool install --global dotnet-trace
dotnet build -c Release src/PgOutput2Json.Benchmark
dotnet-trace collect --show-child-io --format speedscope -- dotnet src/PgOutput2Json.Benchmark/bin/Release/net8.0/PgOutput2Json.Benchmark.dll --scenario stream --output clickhouse
```

Open the `.speedscope.json` file at [speedscope.app](https://www.speedscope.app/), or the `.nettrace` file in Visual Studio or PerfView. The trace also covers starting the containers and loading the rows: look at the time after "Running ...". The sample profiler of .NET stops the threads where the runtime can suspend them, so the time of a method can show up in another one: check a finding by measuring the change. For the allocations, use `--profile gc-verbose`, and [dotnet-counters](https://learn.microsoft.com/dotnet/core/diagnostics/dotnet-counters) shows the GC and CPU while it runs:

```
dotnet-counters monitor -- dotnet src/PgOutput2Json.Benchmark/bin/Release/net8.0/PgOutput2Json.Benchmark.dll --rows 5000000
```
