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

        /// <summary>
        /// False if publishers of the output must not write at the same time, e.g. to one file. The initial copy of
        /// a table then uses one worker, whatever <see cref="ReplicationListenerOptions.CopyWorkersPerTable"/> says.
        /// </summary>
        public bool SupportsParallelCopy => true;

        /// <summary>
        /// Called when the listener is built, with the options as configured: throws if the output cannot work with them.
        /// </summary>
        public void Validate(ReplicationListenerOptions listenerOptions)
        {
        }
    }
}
