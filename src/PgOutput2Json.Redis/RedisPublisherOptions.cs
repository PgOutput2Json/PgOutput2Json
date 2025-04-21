using StackExchange.Redis;

namespace PgOutput2Json.Redis
{
    public enum StreamNameSuffix
    {
        None,
        TableName,
        TableNameAndPartition
    }

    public enum PublishMode
    {
        Stream,
        Channel
    }

    public class RedisPublisherOptions
    {
        /// <summary>
        /// Chanel name. Default is 'pgoutput2json'.
        /// </summary>
        public string StreamName { get; set; } = "pgoutput2json";

        /// <summary>
        /// Publish to Stream or Channel. Default is Stream.
        /// </summary>
        public PublishMode PublishMode { get; set; } = PublishMode.Stream;

        /// <summary>
        /// Suffix to be added to the stream name. Default is None.
        /// - TableName -> stream_name:schema_name:table_name (eg. pgoutput2json:public.test_table)
        /// - TableNameAndPartition -> stream_name:schema_name:table_name:partition_number (eg. pgoutput2json:public.test_table:0)
        /// Note that table name is always schema_name.table_name (schema qualified with dot)
        /// </summary>
        public StreamNameSuffix StreamNameSuffix { get; set; } = StreamNameSuffix.None;

        public ConfigurationOptions Redis { get; set; } = new ConfigurationOptions();
    }
}
