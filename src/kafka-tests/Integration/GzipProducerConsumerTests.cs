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
        private BrokerRouter _router;
        private KafkaConnection _kafkaConnection;

        [SetUp]
        public void Setup()
        {
            var options = new KafkaOptions(IntegrationConfig.IntegrationUri);
            _kafkaConnection = new KafkaConnection(new KafkaTcpSocket(new DefaultTraceLog(), options.KafkaServerUri.First()), options.ResponseTimeoutMs, options.Log);
            _router = new BrokerRouter(options);
        }

        [Test]
        public void EnsureGzipCompressedMessageCanSend()
        {
            //ensure topic exists
            _kafkaConnection.SendAsync(new MetadataRequest { Topics = new List<string>(new[] { IntegrationConfig.IntegrationCompressionTopic }) }).Wait();

            var conn = _router.SelectBrokerRoute(IntegrationConfig.IntegrationCompressionTopic, 0);

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
                                                        new Message {Value = "0", Key = "1"},
                                                        new Message {Value = "1", Key = "1"},
                                                        new Message {Value = "2", Key = "1"}
                                                    }
                                        }
                                }
            };

            var response = conn.Connection.SendAsync(request).Result;
            Assert.That(response.First().Error, Is.EqualTo(0));
        }

        [Test]
        public void EnsureGzipCanDecompressMessageFromKafka()
        {
            var producer = new Producer(_router);

            var offsets = producer.GetTopicOffsetAsync(IntegrationConfig.IntegrationCompressionTopic).Result;

            var consumer = new Consumer(new ConsumerOptions(IntegrationConfig.IntegrationCompressionTopic, _router),
                offsets.Select(x => new OffsetPosition(x.PartitionId, 0)).ToArray());
            
            var results = consumer.Consume().Take(3).ToList();

            for (int i = 0; i < 3; i++)
            {
                Assert.That(results[i].Value, Is.EqualTo(i.ToString()));
            }
        }
    }
}
