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
        private readonly KafkaOptions _options = new KafkaOptions { Hosts = new [] {IntegrationConfig.IntegrationUri} };

        private KafkaConnection GetKafkaConnection()
        {
            var log = new DefaultTraceLog();
            return new KafkaConnection(new KafkaTcpSocket(log, _options.Hosts.First()), (int)_options.Timeout.TotalMilliseconds, log);
        }

        [Test]
        public void EnsureGzipCompressedMessageCanSend()
        {
            //ensure topic exists
            using (var conn = GetKafkaConnection())
            {
                conn.SendAsync(new MetadataRequest { Topics = new List<string>(new[] { IntegrationConfig.IntegrationCompressionTopic }) }).Wait(TimeSpan.FromSeconds(10));
            }

            var log = new DefaultTraceLog();
            using (var router = new BrokerRouter(_options, log, new DefaultPartitionSelector(), new DefaultKafkaConnectionFactory(log)))
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
        }

        [Test]
        public void EnsureGzipCanDecompressMessageFromKafka()
        {
            var log = new DefaultTraceLog();
            var router = new BrokerRouter(_options, log, new DefaultPartitionSelector(), new DefaultKafkaConnectionFactory(log));
            var producer = new Producer(router, _options);

            var offsets = producer.GetTopicOffsetAsync(IntegrationConfig.IntegrationCompressionTopic).Result;

            var consumer = new Consumer(router, log, new ConsumerOptions(IntegrationConfig.IntegrationCompressionTopic),
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
