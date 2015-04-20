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
    class FailureTests
    {
        [Test]
        public void TestClusterCanBeManaged()
        {
            using (var cluster = new KafkaTestCluster("server.home", 3))
            {
                cluster.CreateTopic("test", 2, 2);
                cluster.DeleteTopic("test");
            }
        }
        [Test]
        public async Task TestManagedClusterWorks()
        {
            using (var cluster = new KafkaTestCluster("server.home", 1))
            {
                var topic = "test";
                cluster.CreateTopic(topic);
                using (var brokers = new KafkaBrokers(cluster.CreateBrokerUris()))
                {
                    var producer = KafkaProducer.Create(brokers, new StringSerializer());
                    await producer.SendAsync(KeyedMessage.Create(topic, "Test"), CancellationToken.None).ConfigureAwait(true);

                    var consumer = KafkaConsumer.Create(topic, brokers, new StringSerializer(),
                        new TopicSelector { Topic = topic, Partition = 0, Offset = 0 });
                    var result = await consumer.ReceiveAsync(CancellationToken.None).ConfigureAwait(true);

                    Assert.That(result, Has.Count.EqualTo(1));
                    var first = result[0];

                    Assert.That(first.Value, Is.EqualTo("Test"));
                    Assert.That(first.Offset, Is.EqualTo(0));

                }
                cluster.DeleteTopic(topic);
            }
        }


        [Test]
        public async Task VerifyABrokerStoppingAndRestartingCanBeHandledByTheConsumer()
        {
            using (var cluster = new KafkaTestCluster("server.home", 1))
            {
                var topic = "test";
                cluster.CreateTopic(topic);
                using (var brokers = new KafkaBrokers(cluster.CreateBrokerUris()))
                {
                    var producer = KafkaProducer.Create(brokers, new StringSerializer());
                    await producer.SendAsync(KeyedMessage.Create(topic, "Test"), CancellationToken.None);

                    await Task.Delay(1000);
                    cluster.StopKafkaBroker(0);
                    cluster.RestartKafkaBroker(0);

                    var consumer = KafkaConsumer.Create(topic, brokers, new StringSerializer(),
                        new TopicSelector { Topic = topic, Partition = 0, Offset = 0 });
                    var result = await consumer.ReceiveAsync(CancellationToken.None);

                    Assert.That(result, Has.Count.EqualTo(1));
                    var first = result[0];

                    Assert.That(first.Value, Is.EqualTo("Test"));
                    Assert.That(first.Offset, Is.EqualTo(0));

                }
                cluster.DeleteTopic(topic);
            }
        }

        [Test]
        public async Task VerifyABrokerStoppingAndRestartingCanBeHandledByTheProducer()
        {
            using (var cluster = new KafkaTestCluster("server.home", 1))
            {
                var topic = "test";
                cluster.CreateTopic(topic);
                using (var brokers = new KafkaBrokers(cluster.CreateBrokerUris()))
                {
                    {
                        var producer = KafkaProducer.Create(brokers, new StringSerializer());
                        await producer.SendAsync(KeyedMessage.Create(topic, "Test 0"), CancellationToken.None).ConfigureAwait(true);
                    }


                    {
                        var consumer = KafkaConsumer.Create(topic, brokers, new StringSerializer(),
                            new TopicSelector { Topic = topic, Partition = 0, Offset = 0 });
                        var result = await consumer.ReceiveAsync(CancellationToken.None).ConfigureAwait(true);

                        Assert.That(result, Has.Count.EqualTo(1));
                        var first = result[0];

                        Assert.That(first.Value, Is.EqualTo("Test 0"));
                        Assert.That(first.Offset, Is.EqualTo(0));
                    }

                    cluster.StopKafkaBroker(0);
                    cluster.RestartKafkaBroker(0);

                    {
                        var producer = KafkaProducer.Create(brokers, new StringSerializer());
                        await producer.SendAsync(KeyedMessage.Create(topic, "Test 1"), CancellationToken.None).ConfigureAwait(true);
                    }


                    {
                        var consumer = KafkaConsumer.Create(topic, brokers, new StringSerializer(),
                            new TopicSelector { Topic = topic, Partition = 0, Offset = 0 });
                        var result = await consumer.ReceiveAsync(CancellationToken.None).ConfigureAwait(true);

                        Assert.That(result, Has.Count.EqualTo(2));
                        var first = result[0];

                        Assert.That(first.Value, Is.EqualTo("Test 0"));
                        Assert.That(first.Offset, Is.EqualTo(0));

                        var second = result[1];
                        Assert.That(second.Value, Is.EqualTo("Test 1"));
                        Assert.That(second.Offset, Is.EqualTo(1));

                    }


                }
                cluster.DeleteTopic(topic);
            }
        }
    }
}
