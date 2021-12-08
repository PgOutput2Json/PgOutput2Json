using Microsoft.Extensions.Logging;

namespace PgOutput2Json.Core
{
    public interface IMessagePublisherFactory
    {
        public IMessagePublisher CreateMessagePublisher(ILoggerFactory? loggerFactory);
    }
}
