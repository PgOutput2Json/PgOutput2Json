using Microsoft.Extensions.Logging;

namespace PgOutput2Json
{
    public interface IMessagePublisherFactory
    {
        public IMessagePublisher CreateMessagePublisher(ReplicationListenerOptions listenerOptions, string slotName, ILoggerFactory? loggerFactory);

        /// <summary>
        /// The write mode the output cannot work without, or null if it takes any. It is set when the listener is built,
        /// whatever <see cref="JsonOptions.WriteMode"/> was configured.
        /// </summary>
        public JsonWriteMode? RequiredWriteMode => null;
    }
}
