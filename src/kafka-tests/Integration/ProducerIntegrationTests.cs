using kafka_tests.Helpers;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using NUnit.Framework;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace kafka_tests.Integration
{
    [TestFixture]
    [Category("Integration")]
    public class ProducerIntegrationTests
    {
        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void ProducerShouldNotExpectResponseWhenAckIsZero()
        {
            using (var router = new BrokerRouter(new KafkaOptions(IntegrationConfig.IntegrationUri)))
            using (var producer = new Producer(router))
            {
                var sendTask = producer.SendMessageAsync(IntegrationConfig.IntegrationTopic, new[] { new Message(Guid.NewGuid().ToString()) }, acks: 0);

                sendTask.Wait(TimeSpan.FromMinutes(2));

                Assert.That(sendTask.Status, Is.EqualTo(TaskStatus.RanToCompletion));
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async void SendAsyncShouldGetOneResultForMessage()
        {
            using (var router = new BrokerRouter(new KafkaOptions(IntegrationConfig.IntegrationUri)))
            using (var producer = new Producer(router))
            {
                var result = await producer.SendMessageAsync(IntegrationConfig.IntegrationTopic, new[] { new Message(Guid.NewGuid().ToString()) });

                Assert.That(result.Count, Is.EqualTo(1));
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async void SendAsyncShouldGetAResultForEachPartitionSentTo()
        {
            using (var router = new BrokerRouter(new KafkaOptions(IntegrationConfig.IntegrationUri)))
            using (var producer = new Producer(router))
            {
                var messages = new[] { new Message("1"), new Message("2"), new Message("3") };
                var result = await producer.SendMessageAsync(IntegrationConfig.IntegrationTopic, messages);

                Assert.That(result.Count, Is.EqualTo(messages.Count()));
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
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

                Assert.That(result.Count, Is.EqualTo(tasks.Count()));
            }
        }
    }
}