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
        private BrokerRouterMock _brokerRouterMock;

        [SetUp]
        public void Setup()
        {
            _kernel = new MoqMockingKernel();
            _brokerRouterMock = new BrokerRouterMock(_kernel);
        }

        [Test]
        public void ProducerShouldGroupMessagesByBroker()
        {
            var router = _brokerRouterMock.CreateBrokerRouter();
            var producer = new Producer(router);

            var messages = new List<Message>
                {
                    new Message{Value = "1"}, new Message{Value = "2"}
                };

            var response = producer.SendMessageAsync("UnitTest", messages).Result;

            Assert.That(response.Count, Is.EqualTo(2));
            _brokerRouterMock.BrokerConn0.Verify(x => x.SendAsync(It.IsAny<IKafkaRequest<ProduceResponse>>()), Times.Once());
            _brokerRouterMock.BrokerConn1.Verify(x => x.SendAsync(It.IsAny<IKafkaRequest<ProduceResponse>>()), Times.Once());
        }
    }
}
