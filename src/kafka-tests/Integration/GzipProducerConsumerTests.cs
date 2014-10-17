using System;
using System.Collections.Generic;
using System.Linq;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using NUnit.Framework;
using kafka_tests.Helpers;

namespace kafka_tests.Integration
{
    [TestFixture]
    [Category("Integration")]
    public class GzipProducerConsumerTests
    {
        private readonly KafkaOptions _options = new KafkaOptions(IntegrationConfig.IntegrationUri);

        private KafkaConnection GetKafkaConnection()
        {
            var endpoint = new DefaultKafkaConnectionFactory().Resolve(_options.KafkaServerUri.First(), _options.Log);
            return new KafkaConnection(new KafkaTcpSocket(new DefaultTraceLog(), endpoint), _options.ResponseTimeoutMs, _options.Log);
        }

        [Test]
        public void EnsureGzipCompressedMessageCanSend()
        {
            //ensure topic exists
            using (var conn = GetKafkaConnection())
            {
                conn.SendAsync(new MetadataRequest { Topics = new List<string>(new[] { IntegrationConfig.IntegrationCompressionTopic }) }).Wait(TimeSpan.FromSeconds(10));
            }

            using (var router = new BrokerRouter(_options))
            {
                var conn = router.SelectBrokerRoute(IntegrationConfig.IntegrationCompressionTopic, 0);

                var request = new ProduceRequest
                {
                    Acks = 1,
                    TimeoutMS = 1000,
                    Payload = new List<Payload>
                                {
                                    new Payload
                                        {
                                            Codec = MessageCodec.CodecGzip,
                                            Topic = IntegrationConfig.IntegrationCompressionTopic,
                                            Partition = 0,
                                            Messages = new List<Message>
                                                    {
                                                        new Message("0", "1"),
                                                        new Message("1", "1"),
                                                        new Message("2", "1")
                                                    }
                                        }
                                }
                };

                var response = conn.Connection.SendAsync(request).Result;
                Assert.That(response.First().Error, Is.EqualTo(0));
            }
        }

        [Test]
        [Ignore("Not working, sometimes crashing server.  Needs review.")]
        public void EnsureGzipCanDecompressMessageFromKafka()
        {
            var router = new BrokerRouter(_options);
            var producer = new Producer(router);

            var offsets = producer.GetTopicOffsetAsync(IntegrationConfig.IntegrationCompressionTopic).Result;

            var consumer = new Consumer(new ConsumerOptions(IntegrationConfig.IntegrationCompressionTopic, router),
                offsets.Select(x => new OffsetPosition(x.PartitionId, 0)).ToArray());

            var results = consumer.Consume().Take(3).ToList();

            for (int i = 0; i < 3; i++)
            {
                Assert.That(results[i].Value, Is.EqualTo(i.ToString()));
            }

            using (producer)
            using (consumer) { }
        }
    }
}
