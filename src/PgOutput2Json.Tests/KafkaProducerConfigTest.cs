using Confluent.Kafka;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using PgOutput2Json.Kafka;

namespace PgOutput2Json.Tests
{
    [TestClass]
    public class KafkaProducerConfigTest
    {
        [TestMethod]
        public void CreateProducerConfig_should_default_to_gapless_delivery()
        {
            var options = new KafkaPublisherOptions();

            var config = KafkaPublisher.CreateProducerConfig(options);

            Assert.AreEqual(true, config.EnableIdempotence);
            Assert.AreEqual(true, config.EnableGaplessGuarantee);
            Assert.AreEqual(0, config.MessageTimeoutMs);
            Assert.AreEqual(Partitioner.Murmur2Random, config.Partitioner);
            Assert.AreEqual("localhost:9092", config.BootstrapServers);
        }

        [TestMethod]
        public void CreateProducerConfig_should_not_modify_the_users_config()
        {
            var options = new KafkaPublisherOptions();

            KafkaPublisher.CreateProducerConfig(options);

            Assert.IsNull(options.ProducerConfig.EnableIdempotence);
            Assert.IsNull(options.ProducerConfig.EnableGaplessGuarantee);
            Assert.IsNull(options.ProducerConfig.MessageTimeoutMs);
        }

        [TestMethod]
        public void CreateProducerConfig_should_keep_explicit_settings()
        {
            var options = new KafkaPublisherOptions
            {
                ProducerConfig = new ProducerConfig
                {
                    EnableIdempotence = true,
                    EnableGaplessGuarantee = false,
                    MessageTimeoutMs = 5000,
                    Partitioner = Partitioner.Consistent,
                }
            };

            var config = KafkaPublisher.CreateProducerConfig(options);

            Assert.AreEqual(true, config.EnableIdempotence);
            Assert.AreEqual(false, config.EnableGaplessGuarantee);
            Assert.AreEqual(5000, config.MessageTimeoutMs);
            Assert.AreEqual(Partitioner.Consistent, config.Partitioner);
        }

        [TestMethod]
        public void CreateProducerConfig_should_not_enable_gapless_without_idempotence()
        {
            var options = new KafkaPublisherOptions
            {
                ProducerConfig = new ProducerConfig { EnableIdempotence = false }
            };

            var config = KafkaPublisher.CreateProducerConfig(options);

            Assert.AreEqual(false, config.EnableIdempotence);
            Assert.IsNull(config.EnableGaplessGuarantee);
        }

        [TestMethod]
        public void CreateProducerConfig_should_keep_the_delivery_timeout_alias()
        {
            var options = new KafkaPublisherOptions();
            options.ProducerConfig.Set("delivery.timeout.ms", "5000");

            var config = KafkaPublisher.CreateProducerConfig(options);

            Assert.IsNull(config.MessageTimeoutMs);
            Assert.AreEqual("5000", config.Get("delivery.timeout.ms"));
        }
    }
}
