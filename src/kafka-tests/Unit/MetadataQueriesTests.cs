using KafkaNet;
using KafkaNet.Protocol;
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
    [Category("Unit")]
    public class MetadataQueriesTest
    {
        private MoqMockingKernel _kernel;

        [SetUp]
        public void Setup()
        {
            _kernel = new MoqMockingKernel();
        }

        #region GetTopicOffset Tests...
        [Test]
        public void GetTopicOffsetShouldQueryEachBroker()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            var router = routerProxy.Create();
            var common = new MetadataQueries(router);

            var result = common.GetTopicOffsetAsync(BrokerRouterProxy.TestTopic).Result;
            Assert.That(routerProxy.BrokerConn0.OffsetRequestCallCount, Is.EqualTo(1));
            Assert.That(routerProxy.BrokerConn1.OffsetRequestCallCount, Is.EqualTo(1));
        }

        [Test]
        public void GetTopicOffsetShouldThrowAnyException()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy.BrokerConn0.OffsetResponseFunction = () => { throw new ApplicationException("test 99"); };
            var router = routerProxy.Create();
            var common = new MetadataQueries(router);

            common.GetTopicOffsetAsync(BrokerRouterProxy.TestTopic).ContinueWith(t =>
            {
                Assert.That(t.IsFaulted, Is.True);
                Assert.That(t.Exception.Flatten().ToString(), Is.StringContaining("test 99"));
            }).Wait();
        } 
        #endregion

        #region GetTopic Tests...
        [Test]
        public void GetTopicShouldReturnTopic()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            var router = routerProxy.Create();
            var common = new MetadataQueries(router);

            var result = common.GetTopic(BrokerRouterProxy.TestTopic);
            Assert.That(result.Name, Is.EqualTo(BrokerRouterProxy.TestTopic));
        }

        [Test]
        [ExpectedException(typeof(InvalidTopicMetadataException))]
        public void EmptyTopicMetadataShouldThrowException()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            var router = routerProxy.Create();
            var common = new MetadataQueries(router);

            common.GetTopic("MissingTopic");
        }

        #endregion

        [Test]
        public void EnsureCommonQueriesDisposesRouter()
        {
            var router = _kernel.GetMock<IBrokerRouter>();
            var common = new MetadataQueries(router.Object);
            using (common) { }
            router.Verify(x => x.Dispose(), Times.Once());
        }
    }
}
