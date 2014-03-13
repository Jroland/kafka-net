using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using NUnit.Framework;

namespace kafka_tests.Integration
{
    [TestFixture]
    public class GzipProducerConsumerTests
    {
        private BrokerRouter _router;

        [SetUp]
        public void Setup()
        {
            var options = new KafkaOptions(new Uri(ConfigurationManager.AppSettings["IntegrationKafkaServerUrl"]));

            _router = new BrokerRouter(options);
        }

        [Test]
        public void EnsureGzipCompressedMessageSendReceived()
        {
            var producer = new Producer(_router);

            var offsets = producer.GetTopicOffsetAsync("LoadTest").Result;

            var consumer = new Consumer(new ConsumerOptions("LoadTest", _router),
                offsets.Select(x => new OffsetPosition(x.PartitionId, x.Offsets.Max())).ToArray());

            var response = producer.SendMessageAsync("LoadTest", new[]
                {
                    new Message {Value = "0", Key = "1"},
                    new Message {Value = "1", Key = "1"},
                    new Message {Value = "2", Key = "1"}
                }, codec: MessageCodec.CodecGzip).Result;

            Assert.That(response.First().Error, Is.EqualTo(0));

            var results = consumer.Consume().Take(3).ToList();

            for (int i = 0; i < 3; i++)
            {
                Assert.That(results[i].Value, Is.EqualTo(i.ToString()));
            }
        }
    }
}
