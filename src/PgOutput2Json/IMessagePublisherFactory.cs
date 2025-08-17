using Microsoft.Extensions.Logging;

namespace PgOutput2Json
{
    public interface IMessagePublisherFactory
    {
        public IMessagePublisher CreateMessagePublisher(ReplicationListenerOptions listenerOptions, string slotName, ILoggerFactory? loggerFactory);
    }
}
