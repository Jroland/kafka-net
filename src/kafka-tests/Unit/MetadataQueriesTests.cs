using kafka_tests.Helpers;
using KafkaNet;
using KafkaNet.Protocol;
using Moq;
using Ninject.MockingKernel.Moq;
using NUnit.Framework;
using System;
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

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void GetTopicOffsetShouldQueryEachBroker()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            var router = routerProxy.Create();
            var common = new MetadataQueries(router);

            var result = common.GetTopicOffsetAsync(BrokerRouterProxy.TestTopic).Result;
            Assert.That(routerProxy.BrokerConn0.OffsetRequestCallCount, Is.EqualTo(1));
            Assert.That(routerProxy.BrokerConn1.OffsetRequestCallCount, Is.EqualTo(1));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
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

        #endregion GetTopicOffset Tests...

        #region GetTopic Tests...

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task GetTopicShouldReturnTopic()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            var router = routerProxy.Create();
            await router.RefreshMissingTopicMetadata(BrokerRouterProxy.TestTopic);
            var common = new MetadataQueries(router);

            var result = common.GetTopicFromCache(BrokerRouterProxy.TestTopic);
            Assert.That(result.Name, Is.EqualTo(BrokerRouterProxy.TestTopic));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [ExpectedException(typeof(InvalidTopicNotExistsInCache))]
        public void EmptyTopicMetadataShouldThrowException()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            var router = routerProxy.Create();
            var common = new MetadataQueries(router);

            common.GetTopicFromCache("MissingTopic");
        }

        #endregion GetTopic Tests...

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void EnsureCommonQueriesDisposesRouter()
        {
            var router = _kernel.GetMock<IBrokerRouter>();
            var common = new MetadataQueries(router.Object);
            using (common) { }
            router.Verify(x => x.Dispose(), Times.Once());
        }
    }
}