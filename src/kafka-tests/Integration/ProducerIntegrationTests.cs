using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using kafka_tests.Helpers;
using NUnit.Framework;

namespace kafka_tests.Integration
{
    [TestFixture]
    [Category("Integration")]
    public class ProducerIntegrationTests
    {
        [Test]
        public void ProducerShouldNotExpectResponseWhenAckIsZero()
        {
            using (var router = new BrokerRouter(new KafkaOptions(IntegrationConfig.IntegrationUri)))
            using (var producer = new Producer(router))
            {
                var sendTask = producer.SendMessageAsync(IntegrationConfig.IntegrationTopic, new[] { new Message(Guid.NewGuid().ToString()) }, acks: 0);

                sendTask.Wait(TimeSpan.FromHours(10));

                Assert.That(sendTask.Status, Is.EqualTo(TaskStatus.RanToCompletion));
            }
        }

        [Test]
        public async void SendAsyncShouldGetOneResultForMessage()
        {
            using (var router = new BrokerRouter(new KafkaOptions(IntegrationConfig.IntegrationUri)))
            using (var producer = new Producer(router))
            {
                var result = await producer.SendMessageAsync(IntegrationConfig.IntegrationTopic, new[] { new Message(Guid.NewGuid().ToString()) });

                Assert.That(result.Count, Is.EqualTo(1));
            }
        }

        [Test]
        public async void SendAsyncShouldGetAResultForEachPartitionSentTo()
        {
            using (var router = new BrokerRouter(new KafkaOptions(IntegrationConfig.IntegrationUri)))
            using (var producer = new Producer(router))
            {
                var result = await producer.SendMessageAsync(IntegrationConfig.IntegrationTopic, new[] { new Message("1"), new Message("2"), new Message("3") });

                Assert.That(result.Count, Is.EqualTo(2));
            }
        }

        [Test]
        public async void SendAsyncShouldGetOneResultForEachPartitionThroughBatching()
        {
            using (var router = new BrokerRouter(new KafkaOptions(IntegrationConfig.IntegrationUri)))
            using (var producer = new Producer(router))
            {
                var tasks = new[]
                {
                    producer.SendMessageAsync(IntegrationConfig.IntegrationTopic, new[] {new Message("1")}),
                    producer.SendMessageAsync(IntegrationConfig.IntegrationTopic, new[] {new Message("1")}),
                    producer.SendMessageAsync(IntegrationConfig.IntegrationTopic, new[] {new Message("1")}),
                };

                await Task.WhenAll(tasks);

                var result = tasks.SelectMany(x => x.Result).Distinct().ToList();

                Assert.That(result.Count, Is.EqualTo(2));
            }
        }
    }
}
