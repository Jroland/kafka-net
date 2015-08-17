using kafka_tests.Helpers;
using KafkaNet;
using KafkaNet.Common;
using KafkaNet.Model;
using KafkaNet.Protocol;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

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

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task EnsureGzipCompressedMessageCanSend()
        {
            using (var conn = GetKafkaConnection())
            {
                conn.SendAsync(new MetadataRequest
                {
                    Topics = new List<string>(new[] { IntegrationConfig.IntegrationCompressionTopic })
                })
                    .Wait(TimeSpan.FromSeconds(10));
            }

            using (var router = new BrokerRouter(_options))
            {
                await router.RefreshMissingTopicMetadata(IntegrationConfig.IntegrationCompressionTopic);
                var conn = router.SelectBrokerRouteFromLocalCache(IntegrationConfig.IntegrationCompressionTopic, 0);

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

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void EnsureGzipCanDecompressMessageFromKafka()
        {
            var router = new BrokerRouter(_options);
            var producer = new Producer(router);

            var offsets = producer.GetTopicOffsetAsync(IntegrationConfig.IntegrationCompressionTopic).Result;

            var consumer = new Consumer(new ConsumerOptions(IntegrationConfig.IntegrationCompressionTopic, router) { PartitionWhitelist = new List<int>() { 0 } },
                offsets.Select(x => new OffsetPosition(x.PartitionId, x.Offsets.Max())).ToArray());
            int numberOfmessage = 3;
            for (int i = 0; i < numberOfmessage; i++)
            {
                producer.SendMessageAsync(IntegrationConfig.IntegrationCompressionTopic, new[] { new Message(i.ToString()) }, codec: MessageCodec.CodecGzip,
              partition: 0);
            }

            var results = consumer.Consume(new CancellationTokenSource(TimeSpan.FromMinutes(1)).Token).Take(numberOfmessage).ToList();

            for (int i = 0; i < numberOfmessage; i++)
            {
                Assert.That(results[i].Value.ToUtf8String(), Is.EqualTo(i.ToString()));
            }

            using (producer)
            using (consumer) { }
        }
    }
}