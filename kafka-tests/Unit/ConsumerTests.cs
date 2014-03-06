using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using Moq;
using Ninject.MockingKernel.Moq;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace kafka_tests.Unit
{
    [TestFixture]
    public class ConsumerTests
    {
        private MoqMockingKernel _kernel;

        [SetUp]
        public void Setup()
        {
            _kernel = new MoqMockingKernel();
        }

        [Test]
        public void ConsumerWhitelistShouldOnlyConsumeSpecifiedPartition()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy.BrokerConn0.FetchResponseFunction = () => { return new FetchResponse(); };
            var router = routerProxy.Create();
            var options = CreateOptions(router);
            options.PartitionWhitelist = new List<int> { 0 };
            var consumer = new Consumer(options);
            
            var test = consumer.Consume().Take(1);
            while (consumer.ConsumerTaskCount <= 0)
            {
                Thread.Sleep(100);
            }

            Assert.That(routerProxy.BrokerConn0.FetchRequestCallCount, Is.GreaterThanOrEqualTo(1));
            Assert.That(routerProxy.BrokerConn1.FetchRequestCallCount, Is.EqualTo(0));
        }

        [Test]
        public void EnsureConsumerDisposesRouter()
        {
            var router = _kernel.GetMock<IBrokerRouter>();
            var consumer = new Consumer(CreateOptions(router.Object));
            using (consumer) { }
            router.Verify(x => x.Dispose(), Times.Once());
        }

        private ConsumerOptions CreateOptions(IBrokerRouter router)
        {
            return new ConsumerOptions { Router = router, Topic = BrokerRouterProxy.TestTopic };
        }
    }
}
