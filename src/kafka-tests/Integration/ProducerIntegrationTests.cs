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
                var sendTask = producer.SendMessageAsync(IntegrationConfig.IntegrationTopic, new[] { new Message(Guid.NewGuid().ToString()) }, acks:0);

                sendTask.Wait(TimeSpan.FromHours(10));

                Assert.That(sendTask.Status, Is.EqualTo(TaskStatus.RanToCompletion));
            }
        }
    }
}
