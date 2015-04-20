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
    class MultiplePartitionTests
    {
        private readonly string defaultConsumerGroup = "unit-tests";

        private KafkaTestCluster testCluster;

        [OneTimeSetUp]
        public void BuildTestCluster()
        {
            testCluster = new KafkaTestCluster("server.home", 1);
        }

        [OneTimeTearDown]
        public void DestroyTestCluster()
        {
            testCluster.Dispose();
            testCluster = null;
        }

        [Test]
        public async Task ProduceToTwoPartitions()
        {
            var keySerializer = new Int32Serializer();
            var valueSerializer = new StringSerializer();
            var messagePartitioner = new Int32Partitioner();

            using (var temporaryTopic = testCluster.CreateTemporaryTopic(partitions:2))
            using (var brokers = new KafkaBrokers(testCluster.CreateBrokerUris()))
            {
                var topic = temporaryTopic.Name;
                var producer = KafkaProducer.Create(brokers, keySerializer, valueSerializer, messagePartitioner);
                var consumers = new[] {
                    KafkaConsumer.Create(defaultConsumerGroup, brokers, keySerializer, valueSerializer,
                    new TopicSelector { Partition = 0, Topic = topic }),
                KafkaConsumer.Create(defaultConsumerGroup, brokers, keySerializer, valueSerializer,
                    new TopicSelector { Partition = 1, Topic = topic })
                };


                await producer.SendAsync(new[] {
                    KeyedMessage.Create(topic, 0, "Message to partition 0"),
                    KeyedMessage.Create(topic, 1, "Message to partition 1")
                }, CancellationToken.None).ConfigureAwait(true);

                for (var i = 0; i < consumers.Length; i++)
                {
                    var responses = await consumers[i].ReceiveAsync(CancellationToken.None).ConfigureAwait(true);
                    Assert.That(responses, Is.Not.Null);
                    Assert.That(responses, Has.Count.EqualTo(1));

                    var first = responses.First();
                    Assert.That(first.Offset, Is.EqualTo(0));
                    Assert.That(first.Partition, Is.EqualTo(i));
                    Assert.That(first.Key, Is.EqualTo(i));
                    Assert.That(first.Topic, Is.EqualTo(topic));
                    Assert.That(first.Value, Is.EqualTo("Message to partition " + i));
                }
            }
        }
        
        [Theory]
        [TestCase(1, 1, 1)]
        [TestCase(1, 2, 2)]
        [TestCase(1, 2, 4)]
        [TestCase(2, 1, 1)]
        [TestCase(2, 2, 2)]
        [TestCase(2, 2, 4)]
        public async Task ProduceToMultiplePartitions(int numberOfPartitions, int numberOfKeys, int numberOfMessages)
        {
            var keySerializer = new Int32Serializer();
            var valueSerializer = new StringSerializer();
            var messagePartitioner = new Int32Partitioner();

            using (var temporaryTopic = testCluster.CreateTemporaryTopic(partitions: 2))
            using (var brokers = new KafkaBrokers(testCluster.CreateBrokerUris()))
            {
                var topic = temporaryTopic.Name;
                {
                    var producer = KafkaProducer.Create(brokers, keySerializer, valueSerializer, messagePartitioner);
                    var messages =
                        Enumerable
                            .Range(0, numberOfMessages)
                            .Select(i => KeyedMessage.Create(topic, i % numberOfKeys, i % numberOfPartitions, "Message " + i));
                    await producer.SendAsync(messages, CancellationToken.None).ConfigureAwait(true);
                }

                {
                    var selectors =
                        Enumerable
                            .Range(0, numberOfPartitions)
                            .Select(partition => new TopicSelector { Partition = partition, Topic = topic })
                            .ToArray();
                    var consumer = KafkaConsumer.Create(defaultConsumerGroup, brokers, keySerializer, valueSerializer, selectors);

                    var responses = await consumer.ReceiveAsync(CancellationToken.None).ConfigureAwait(true);
                    Assert.That(responses, Has.Count.EqualTo(numberOfMessages));
                    var received = new bool[numberOfMessages];
                    var offsets = new long[numberOfPartitions];
                    foreach (var response in responses)
                    {
                        var split = response.Value.Split(' ');
                        Assert.That(split, Has.Length.EqualTo(2));
                        Assert.That(split[0], Is.EqualTo("Message"));
                        int messageNumber;
                        var parsed = Int32.TryParse(split[1], out messageNumber);
                        Assert.That(parsed, Is.True);
                        Assert.That(messageNumber, Is.InRange(0, numberOfMessages - 1));
                        var key = messageNumber % numberOfKeys;
                        Assert.That(response.Key, Is.EqualTo(key));

                        var partition = messageNumber % numberOfPartitions;
                        Assert.That(response.Partition, Is.EqualTo(partition));

                        Assert.That(received[messageNumber], Is.False);
                        received[messageNumber] = true;

                        Assert.That(response.Offset, Is.EqualTo(offsets[response.Partition]));
                        offsets[response.Partition] += 1;

                        Assert.That(response.Topic, Is.EqualTo(topic));

                    }
                }
            }
        }
    }
}
