using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;
using Npgsql;

namespace PgOutput2Json.IntegrationTests
{
    internal sealed record PublishedMessage(string Json, string Table, string Key, string PartitionKey, ulong TxFinalLsn, ulong MessageNo, bool IsTableSync = false);

    internal static class Db
    {
        public static async Task ExecuteAsync(string sql)
        {
            await using var connection = new NpgsqlConnection(Containers.PgConnectionString);
            await connection.OpenAsync();

            await using var command = new NpgsqlCommand(sql, connection);
            await command.ExecuteNonQueryAsync();
        }

        public static async Task<T> ScalarAsync<T>(string sql)
        {
            await using var connection = new NpgsqlConnection(Containers.PgConnectionString);
            await connection.OpenAsync();

            await using var command = new NpgsqlCommand(sql, connection);
            return (T)(await command.ExecuteScalarAsync())!;
        }

        /// <summary>
        /// The slot is created before the listener starts, so the changes made right after
        /// this call are captured no matter when the listener connects.
        /// </summary>
        public static Task CreateSlotAsync(string slotName)
        {
            return ExecuteAsync($"SELECT pg_create_logical_replication_slot('{slotName}', 'pgoutput')");
        }

        /// <summary>
        /// The stored table states of a slot, as "table:state,table:state" ordered by table name.
        /// </summary>
        public static async Task<string> TableStatesAsync(string slotName)
        {
            if (!await ScalarAsync<bool>("SELECT to_regclass('pgoutput2json.table_sync_state') IS NOT NULL")) return "";

            return await ScalarAsync<string>($@"
                SELECT coalesce(string_agg(table_name || ':' || state, ',' ORDER BY table_name), '')
                FROM pgoutput2json.table_sync_state WHERE slot_name = '{slotName}'");
        }
    }

    internal static class Wait
    {
        public static async Task UntilAsync(Func<Task<bool>> condition, string what, int timeoutSeconds = 30)
        {
            var stopwatch = Stopwatch.StartNew();

            while (!await condition())
            {
                if (stopwatch.Elapsed > TimeSpan.FromSeconds(timeoutSeconds))
                {
                    throw new TimeoutException($"Timed out waiting for {what}");
                }

                await Task.Delay(50);
            }
        }

        public static async Task UntilAsync(Func<bool> condition, string what, int timeoutSeconds = 30)
        {
            var stopwatch = Stopwatch.StartNew();

            while (!condition())
            {
                if (stopwatch.Elapsed > TimeSpan.FromSeconds(timeoutSeconds))
                {
                    throw new TimeoutException($"Timed out waiting for {what}");
                }

                await Task.Delay(50);
            }
        }
    }

    /// <summary>
    /// Runs a listener in the background until disposed.
    /// </summary>
    internal sealed class RunningListener : IAsyncDisposable
    {
        private readonly IPgOutput2Json _pgOutput2Json;
        private readonly CancellationTokenSource _cts = new();
        private readonly Task _task;

        public RunningListener(PgOutput2JsonBuilder builder)
        {
            _pgOutput2Json = builder.Build();
            _task = Task.Run(() => _pgOutput2Json.StartAsync(_cts.Token));
        }

        public async ValueTask DisposeAsync()
        {
            _cts.Cancel();

            try
            {
                await _task;
            }
            catch (OperationCanceledException)
            {
            }

            _pgOutput2Json.Dispose();
            _cts.Dispose();
        }
    }

    /// <summary>
    /// Collects the published messages and remembers the last confirmed position across
    /// reconnects and restarts, the way a real publisher reads it back from its broker.
    /// </summary>
    internal sealed class CollectingPublisherFactory : IMessagePublisherFactory
    {
        private readonly ConcurrentQueue<PublishedMessage> _messages = new();
        private (ulong, ulong) _lastConfirmed;

        public List<PublishedMessage> Messages => _messages.ToList();

        /// <summary>
        /// Slows down a table sync, so that the main stream gets ahead of it.
        /// </summary>
        public TimeSpan SyncConfirmDelay { get; set; }

        /// <summary>
        /// Called before a message is collected, and before a batch is confirmed. Throw to make the publisher fail.
        /// </summary>
        public Action<JsonMessage>? OnPublish { get; set; }
        public Action? OnConfirm { get; set; }

        /// <summary>
        /// The number of messages in each confirmed batch.
        /// </summary>
        public List<int> ConfirmedBatches { get { lock (_messages) return _confirmedBatches.ToList(); } }

        /// <summary>
        /// The number of publishers created: one per connection of the listener, and one per table sync.
        /// </summary>
        public int PublisherCount => _publisherCount;

        private readonly List<int> _confirmedBatches = [];
        private int _publisherCount;

        public (ulong TxFinalLsn, ulong MessageNo) LastConfirmed
        {
            get { lock (_messages) return _lastConfirmed; }
        }

        public IMessagePublisher CreateMessagePublisher(ReplicationListenerOptions listenerOptions, string slotName, ILoggerFactory? loggerFactory)
        {
            Interlocked.Increment(ref _publisherCount);

            return new Publisher(this);
        }

        private sealed class Publisher(CollectingPublisherFactory owner) : MessagePublisher
        {
            private (ulong, ulong) _lastPublished;
            private bool _hasSyncMessages;
            private int _unconfirmed;

            public override Task PublishAsync(JsonMessage message, CancellationToken token)
            {
                owner.OnPublish?.Invoke(message);

                _unconfirmed++;

                owner._messages.Enqueue(new PublishedMessage(
                    message.Json.ToString(),
                    message.TableName.ToString(),
                    message.KeyKolValue.ToString(),
                    message.PartitionKolValue.ToString(),
                    message.TxFinalLsn,
                    message.MessageNo,
                    message.IsTableSync));

                // the messages of a table sync must not move the position
                if (message.IsTableSync)
                {
                    _hasSyncMessages = true;
                }
                else
                {
                    _lastPublished = (message.TxFinalLsn, message.MessageNo);
                }

                return Task.CompletedTask;
            }

            public override async Task ConfirmAsync(CancellationToken token)
            {
                owner.OnConfirm?.Invoke();

                if (_unconfirmed > 0)
                {
                    lock (owner._messages) owner._confirmedBatches.Add(_unconfirmed);

                    _unconfirmed = 0;
                }

                if (_hasSyncMessages && owner.SyncConfirmDelay > TimeSpan.Zero)
                {
                    await Task.Delay(owner.SyncConfirmDelay, token);
                }

                _hasSyncMessages = false;

                if (_lastPublished == default) return;

                lock (owner._messages) owner._lastConfirmed = _lastPublished;
            }

            public override Task<(ulong, ulong)> GetLastPublishedWalSeqAsync(CancellationToken token)
            {
                _lastPublished = owner.LastConfirmed;

                return Task.FromResult(_lastPublished);
            }

            public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
        }
    }

    /// <summary>
    /// Keeps the warnings and errors logged by the library, with the exception text appended.
    /// </summary>
    internal sealed class CollectingLoggerFactory : ILoggerFactory
    {
        private readonly ConcurrentQueue<(LogLevel Level, string Text)> _entries = new();

        public bool Contains(LogLevel level, string text)
        {
            return _entries.Any(x => x.Level == level && x.Text.Contains(text, StringComparison.Ordinal));
        }

        public override string ToString() => string.Join(Environment.NewLine, _entries.Select(x => $"{x.Level}: {x.Text}"));

        public void AddProvider(ILoggerProvider provider)
        {
        }

        public ILogger CreateLogger(string categoryName) => new Logger(this);

        public void Dispose()
        {
        }

        private sealed class Logger(CollectingLoggerFactory owner) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Warning;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
                owner._entries.Enqueue((logLevel, formatter(state, exception) + " " + exception));
            }
        }
    }
}
