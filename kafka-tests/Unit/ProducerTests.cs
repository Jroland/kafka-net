using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using Moq;
using NUnit.Framework;
using Ninject.MockingKernel.Moq;

namespace kafka_tests.Unit
{
    [TestFixture]
    public class ProducerTests
    {
        private MoqMockingKernel _kernel;
        private BrokerRouterProxy _routerProxy;

        [SetUp]
        public void Setup()
        {
            _kernel = new MoqMockingKernel();
            _routerProxy = new BrokerRouterProxy(_kernel);
        }

        [Test]
        public void ProducerShouldGroupMessagesByBroker()
        {
            var router = _routerProxy.Create();
            var producer = new Producer(router);

            var messages = new List<Message>
                {
                    new Message{Value = "1"}, new Message{Value = "2"}
                };

            var response = producer.SendMessageAsync("UnitTest", messages).Result;

            Assert.That(response.Count, Is.EqualTo(2));
            Assert.That(_routerProxy.BrokerConn0.ProduceRequestCallCount, Is.EqualTo(1));
            Assert.That(_routerProxy.BrokerConn1.ProduceRequestCallCount, Is.EqualTo(1));
        }
    }
}
