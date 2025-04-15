using System.Diagnostics.Metrics;

namespace PgOutput2Json
{
    internal class MetricsHelper
    {
        // Create a Meter (metrics instrument container)
        private static readonly Meter _meter = new("PgOutput2Json", "1.0.0");

        // Define counters for publish and error counts
        private static readonly Counter<long> _publishCount = _meter.CreateCounter<long>("publish_count");
        private static readonly Counter<long> _errorCount = _meter.CreateCounter<long>("error_count");

        public static void IncrementPublishCounter()
        {
            _publishCount.Add(1);
        }
        
        public static void IncrementErrorCounter()
        {
            _errorCount.Add(1);
        }
    }
}
