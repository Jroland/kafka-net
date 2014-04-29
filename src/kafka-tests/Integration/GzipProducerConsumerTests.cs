using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using NUnit.Framework;

namespace kafka_tests.Integration
{
    [TestFixture]
    [Category("Integration")]
    public class GzipProducerConsumerTests
    {
        private BrokerRouter _router;
        private const string CompressTopic = "CompressTest";

        [SetUp]
        public void Setup()
        {
            var options = new KafkaOptions(new Uri(ConfigurationManager.AppSettings["IntegrationKafkaServerUrl"]));

            _router = new BrokerRouter(options);
        }

        [Test]
        [Ignore]
        public void EnsureGzipCompressedMessageCanSend()
        {
            var conn = _router.SelectBrokerRoute(CompressTopic, 0);

            var request = new ProduceRequest
            {
                Acks = 1,
                TimeoutMS = 1000,
                Payload = new List<Payload>
                                {
                                    new Payload
                                        {
                                            Codec = MessageCodec.CodecGzip,
                                            Topic = CompressTopic,
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

            //var offsets = producer.GetTopicOffsetAsync("NewTopic").Result;

            //var consumer = new Consumer(new ConsumerOptions("NewTopic", _router),
            //    offsets.Select(x => new OffsetPosition(x.PartitionId, x.Offsets.Max())).ToArray());

            //var response = producer.SendMessageAsync("NewTopic", new[]
            //    {
            //        new Message {Value = "0", Key = "1"},
            //        new Message {Value = "1", Key = "1"},
            //        new Message {Value = "2", Key = "1"}
            //    }, codec: MessageCodec.CodecGzip).Result;

            //Assert.That(response.First().Error, Is.EqualTo(0));

            //var results = consumer.Consume().Take(3).ToList();

            //for (int i = 0; i < 3; i++)
            //{
            //    Assert.That(results[i].Value, Is.EqualTo(i.ToString()));
            //}
        }

        [Test]
        [Ignore]
        public void EnsureGzipCanDecompressMessageFromKafka()
        {
            var producer = new Producer(_router);

            var offsets = producer.GetTopicOffsetAsync(CompressTopic).Result;

            var consumer = new Consumer(new ConsumerOptions("Empty", _router),
                offsets.Select(x => new OffsetPosition(x.PartitionId, 0)).ToArray());
            
            var results = consumer.Consume().Take(3).ToList();

            for (int i = 0; i < 3; i++)
            {
                Assert.That(results[i].Value, Is.EqualTo(i.ToString()));
            }
        }
    }
}
