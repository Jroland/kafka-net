using NUnit.Framework;
using SimpleKafka;
using SimpleKafkaTests.Helpers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleKafkaTests.Integration
{
    [TestFixture]
    [Category("Integration")]
    class ConsumerTests
    {
        [SetUp]
        public void Setup()
        {
            IntegrationHelpers.zookeeperHost = "zookeeper:2181";
            IntegrationHelpers.dockerOptions = "--link compose_zookeeper_1:zookeeper";
        }
        [Test]
        public async Task TestSimpleConsumerWorksOk()
        {
            var keySerializer = new NullSerializer<string>();
            var valueSerializer = new StringSerializer();
            var messagePartitioner = new LoadBalancedPartitioner<string, string>(1);

            using (var temporaryTopic = IntegrationHelpers.CreateTemporaryTopic())
            using (var brokers = new KafkaBrokers(IntegrationConfig.IntegrationUriArray))
            {
                var topic = temporaryTopic.Topic;
                var producer = new KafkaProducer<string, string>(brokers, keySerializer, valueSerializer, messagePartitioner);
                var consumer = new KafkaConsumer<string, string>(brokers, keySerializer, valueSerializer, new TopicSelector { Partition = 0, Topic = topic });

                await producer.SendAsync(new KafkaMessage<string, string>(topic, null, "Message"), CancellationToken.None).ConfigureAwait(true);

                var responses = await consumer.ReceiveAsync(CancellationToken.None).ConfigureAwait(true);
                Assert.That(responses, Is.Not.Null);
                Assert.That(responses, Has.Count.EqualTo(1));

                var first = responses.First();
                Assert.That(first.Key, Is.Null);
                Assert.That(first.Offset, Is.EqualTo(0));
                Assert.That(first.Partition, Is.EqualTo(0));
                Assert.That(first.Topic, Is.EqualTo(topic));
                Assert.That(first.Value, Is.EqualTo("Message"));
            }
        }

    }
}
