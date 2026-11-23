using System.Threading.Tasks;

using RabbitMQ.Stream.Client.Reliable;

namespace PgOutput2Json.RabbitMqStreams
{
    /// <summary>
    /// The producer closes when its connection drops, instead of reconnecting: the client does not send again the
    /// messages that were in flight, so the ones sent after a reconnect would be in the stream after a gap. The
    /// batch fails instead, and the listener connects again with a new publisher, from the last confirmed position.
    /// </summary>
    internal sealed class NoReconnectStrategy : IReconnectStrategy
    {
        public ValueTask<bool> WhenDisconnected(string itemIdentifier) => ValueTask.FromResult(false);

        public ValueTask WhenConnected(string itemIdentifier) => ValueTask.CompletedTask;
    }
}
