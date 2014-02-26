using KafkaNet;
using Moq;
using Ninject.MockingKernel.Moq;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace kafka_tests.Unit
{
    [TestFixture]
    public class BrokerRouterTests
    {
        private MoqMockingKernel _kernel;

        [SetUp]
        public void Setup()
        {
            _kernel = new MoqMockingKernel();
        }
        
        [Test]
        public void BrokerRouterCanConstruct()
        {
            var conn = _kernel.GetMock<IKafkaConnection>();
            var factory = _kernel.GetMock<IKafkaConnectionFactory>();
            factory.Setup(x => x.Create(It.IsAny<Uri>(), It.IsAny<int>(), It.IsAny<IKafkaLog>())).Returns(() => conn.Object);

            var result = new BrokerRouter(new KafkaNet.Model.KafkaOptions
            {
                KafkaServerUri = new List<Uri> { new Uri("http://localhost:9999") },
                KafkaConnectionFactory = factory.Object
            });
        }
    }
}
