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
        private readonly string defaultConsumerGroup = "unit-tests";

        [SetUp]
        public void Setup()
        {
            IntegrationHelpers.zookeeperHost = "server.home:32181";
        }
        [Test]
        public async Task TestSimpleConsumerWorksOk()
        {
            var keySerializer = new NullSerializer<object>();
            var valueSerializer = new StringSerializer();
            var messagePartitioner = new LoadBalancedPartitioner<object>();

            using (var temporaryTopic = IntegrationHelpers.CreateTemporaryTopic())
            using (var brokers = new KafkaBrokers(IntegrationConfig.IntegrationUriArray))
            {
                var topic = temporaryTopic.Topic;
                var producer = KafkaProducer.Create(brokers, keySerializer, valueSerializer, messagePartitioner);
                var consumer = KafkaConsumer.Create(defaultConsumerGroup, brokers, keySerializer, valueSerializer, 
                    new TopicSelector { Partition = 0, Topic = topic });

                await producer.SendAsync(KeyedMessage.Create(topic, "Message"), CancellationToken.None).ConfigureAwait(true);

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

        [Test]
        public async Task TestProducing3MessagesAllowsTheConsumerToChooseTheCorrectMessage()
        {
            var valueSerializer = new StringSerializer();

            using (var temporaryTopic = IntegrationHelpers.CreateTemporaryTopic())
            using (var brokers = new KafkaBrokers(IntegrationConfig.IntegrationUriArray))
            {
                var topic = temporaryTopic.Topic;
                {
                    var producer = KafkaProducer.Create(brokers, valueSerializer);

                    await producer.SendAsync(new[] {
                        KeyedMessage.Create(topic, "1"),
                        KeyedMessage.Create(topic, "2"),
                        KeyedMessage.Create(topic, "3"),
                        }, CancellationToken.None).ConfigureAwait(true);
                }

                {
                    var earliest = KafkaConsumer.Create(defaultConsumerGroup, brokers, valueSerializer, 
                        new TopicSelector { Partition = 0, Topic = topic, DefaultOffsetSelection = OffsetSelectionStrategy.Earliest });

                    var responses = await earliest.ReceiveAsync(CancellationToken.None).ConfigureAwait(true);
                    Assert.That(responses, Is.Not.Null);
                    Assert.That(responses, Has.Count.EqualTo(3));

                    var first = responses.First();
                    Assert.That(first.Key, Is.Null);
                    Assert.That(first.Offset, Is.EqualTo(0));
                    Assert.That(first.Partition, Is.EqualTo(0));
                    Assert.That(first.Topic, Is.EqualTo(topic));
                    Assert.That(first.Value, Is.EqualTo("1"));
                }

                {
                    var latest = KafkaConsumer.Create(defaultConsumerGroup, brokers, valueSerializer, 
                        new TopicSelector { Partition = 0, Topic = topic, DefaultOffsetSelection = OffsetSelectionStrategy.Last });

                    var responses = await latest.ReceiveAsync(CancellationToken.None).ConfigureAwait(true);
                    Assert.That(responses, Is.Not.Null);
                    Assert.That(responses, Has.Count.EqualTo(1));

                    var first = responses.First();
                    Assert.That(first.Key, Is.Null);
                    Assert.That(first.Offset, Is.EqualTo(2));
                    Assert.That(first.Partition, Is.EqualTo(0));
                    Assert.That(first.Topic, Is.EqualTo(topic));
                    Assert.That(first.Value, Is.EqualTo("3"));
                }

                {
                    var latest = KafkaConsumer.Create(defaultConsumerGroup, brokers, valueSerializer,
                        new TopicSelector { Partition = 0, Topic = topic, DefaultOffsetSelection = OffsetSelectionStrategy.Next });

                    var responses = await latest.ReceiveAsync(CancellationToken.None).ConfigureAwait(true);
                    Assert.That(responses, Is.Not.Null);
                    Assert.That(responses, Has.Count.EqualTo(0));

                }

                {
                    var specified = KafkaConsumer.Create(defaultConsumerGroup, brokers, valueSerializer,
                        new TopicSelector { Partition = 0, Topic = topic, DefaultOffsetSelection = OffsetSelectionStrategy.Specified, Offset = 1 });

                    var responses = await specified.ReceiveAsync(CancellationToken.None).ConfigureAwait(true);
                    Assert.That(responses, Is.Not.Null);
                    Assert.That(responses, Has.Count.EqualTo(2));

                    var first = responses.First();
                    Assert.That(first.Key, Is.Null);
                    Assert.That(first.Offset, Is.EqualTo(1));
                    Assert.That(first.Partition, Is.EqualTo(0));
                    Assert.That(first.Topic, Is.EqualTo(topic));
                    Assert.That(first.Value, Is.EqualTo("2"));
                }
            
            }

        }

        [Test]
        public async Task TestProducing3MessagesAllowsTheConsumerToCommitAndRestart()
        {
            var valueSerializer = new StringSerializer();

            using (var temporaryTopic = IntegrationHelpers.CreateTemporaryTopic())
            using (var brokers = new KafkaBrokers(IntegrationConfig.IntegrationUriArray))
            {
                var topic = temporaryTopic.Topic;
                {
                    var producer = KafkaProducer.Create(brokers, valueSerializer);

                    await producer.SendAsync(new[] {
                        KeyedMessage.Create(topic, "1"),
                        KeyedMessage.Create(topic, "2"),
                        KeyedMessage.Create(topic, "3"),
                        }, CancellationToken.None).ConfigureAwait(true);
                }

                {
                    var noPreviousCommits = KafkaConsumer.Create(defaultConsumerGroup, brokers, valueSerializer,
                        new TopicSelector { Partition = 0, Topic = topic, 
                            DefaultOffsetSelection = OffsetSelectionStrategy.NextUncommitted, 
                            FailureOffsetSelection = OffsetSelectionStrategy.Earliest });

                    var responses = await noPreviousCommits.ReceiveAsync(CancellationToken.None).ConfigureAwait(true);
                    Assert.That(responses, Is.Not.Null);
                    Assert.That(responses, Has.Count.EqualTo(3));

                    var first = responses.First();
                    Assert.That(first.Key, Is.Null);
                    Assert.That(first.Offset, Is.EqualTo(0));
                    Assert.That(first.Partition, Is.EqualTo(0));
                    Assert.That(first.Topic, Is.EqualTo(topic));
                    Assert.That(first.Value, Is.EqualTo("1"));

                    await noPreviousCommits.CommitAsync(new[] { 
                        new TopicPartitionOffset { Topic = topic, Partition = 0, Offset = 0 } 
                    }, CancellationToken.None).ConfigureAwait(true); ;
                }

                {
                    var previousCommit = KafkaConsumer.Create(defaultConsumerGroup, brokers, valueSerializer,
                        new TopicSelector
                        {
                            Partition = 0,
                            Topic = topic,
                            DefaultOffsetSelection = OffsetSelectionStrategy.NextUncommitted,
                            FailureOffsetSelection = OffsetSelectionStrategy.Earliest
                        });

                    var responses = await previousCommit.ReceiveAsync(CancellationToken.None).ConfigureAwait(true);
                    Assert.That(responses, Is.Not.Null);
                    Assert.That(responses, Has.Count.EqualTo(2));

                    var first = responses.First();
                    Assert.That(first.Key, Is.Null);
                    Assert.That(first.Offset, Is.EqualTo(1));
                    Assert.That(first.Partition, Is.EqualTo(0));
                    Assert.That(first.Topic, Is.EqualTo(topic));
                    Assert.That(first.Value, Is.EqualTo("2"));

                }

                {
                    var previousCommitAgain = KafkaConsumer.Create(defaultConsumerGroup, brokers, valueSerializer,
                        new TopicSelector
                        {
                            Partition = 0,
                            Topic = topic,
                            DefaultOffsetSelection = OffsetSelectionStrategy.NextUncommitted,
                            FailureOffsetSelection = OffsetSelectionStrategy.Earliest
                        });

                    var responses = await previousCommitAgain.ReceiveAsync(CancellationToken.None).ConfigureAwait(true);
                    Assert.That(responses, Is.Not.Null);
                    Assert.That(responses, Has.Count.EqualTo(2));

                    var first = responses.First();
                    Assert.That(first.Key, Is.Null);
                    Assert.That(first.Offset, Is.EqualTo(1));
                    Assert.That(first.Partition, Is.EqualTo(0));
                    Assert.That(first.Topic, Is.EqualTo(topic));
                    Assert.That(first.Value, Is.EqualTo("2"));

                    await previousCommitAgain.CommitAsync(new[] { 
                        new TopicPartitionOffset { Topic = topic, Partition = 0, Offset = 1 } 
                    }, CancellationToken.None).ConfigureAwait(true); ;
                }

                {
                    var secondCommit = KafkaConsumer.Create(defaultConsumerGroup, brokers, valueSerializer,
                        new TopicSelector
                        {
                            Partition = 0,
                            Topic = topic,
                            DefaultOffsetSelection = OffsetSelectionStrategy.NextUncommitted,
                            FailureOffsetSelection = OffsetSelectionStrategy.Earliest
                        });

                    var responses = await secondCommit.ReceiveAsync(CancellationToken.None).ConfigureAwait(true);
                    Assert.That(responses, Is.Not.Null);
                    Assert.That(responses, Has.Count.EqualTo(1));

                    var first = responses.First();
                    Assert.That(first.Key, Is.Null);
                    Assert.That(first.Offset, Is.EqualTo(2));
                    Assert.That(first.Partition, Is.EqualTo(0));
                    Assert.That(first.Topic, Is.EqualTo(topic));
                    Assert.That(first.Value, Is.EqualTo("3"));

                    await secondCommit.CommitAsync(new[] { 
                        new TopicPartitionOffset { Topic = topic, Partition = 0, Offset = 2 } 
                    }, CancellationToken.None).ConfigureAwait(true); ;
                }

                {
                    var thirdCommit = KafkaConsumer.Create(defaultConsumerGroup, brokers, valueSerializer,
                        new TopicSelector
                        {
                            Partition = 0,
                            Topic = topic,
                            DefaultOffsetSelection = OffsetSelectionStrategy.NextUncommitted,
                            FailureOffsetSelection = OffsetSelectionStrategy.Earliest
                        });

                    var responses = await thirdCommit.ReceiveAsync(CancellationToken.None).ConfigureAwait(true);
                    Assert.That(responses, Is.Not.Null);
                    Assert.That(responses, Has.Count.EqualTo(0));

                }

            }
        }
    }
}
