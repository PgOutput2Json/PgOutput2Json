using Microsoft.Extensions.Logging;

namespace PgOutput2Json
{
    public interface IMessagePublisherFactory
    {
        public IMessagePublisher CreateMessagePublisher(int batchSize, ILoggerFactory? loggerFactory);
    }
}
