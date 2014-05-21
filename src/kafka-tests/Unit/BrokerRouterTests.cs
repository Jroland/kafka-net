using KafkaNet;
using KafkaNet.Protocol;
using Moq;
using Ninject.MockingKernel.Moq;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace kafka_tests.Unit
{
    [TestFixture]
    [Category("Unit")]
    public class BrokerRouterTests
    {
        private const string TestTopic = "UnitTest";
        private MoqMockingKernel _kernel;
        private Mock<IKafkaConnection> _connMock1;
        private Mock<IKafkaConnectionFactory> _factoryMock;
        private Mock<IPartitionSelector> _partitionSelectorMock;

        [SetUp]
        public void Setup()
        {
            _kernel = new MoqMockingKernel();

            //setup mock IKafkaConnection
            _partitionSelectorMock = _kernel.GetMock<IPartitionSelector>();
            _connMock1 = _kernel.GetMock<IKafkaConnection>();
            _factoryMock = _kernel.GetMock<IKafkaConnectionFactory>();
            _factoryMock.Setup(x => x.Create(It.Is<Uri>(uri => uri.Port == 1), It.IsAny<int>(), It.IsAny<IKafkaLog>())).Returns(() => _connMock1.Object);
        }

        [Test]
        public void BrokerRouterCanConstruct()
        {
            var result = new BrokerRouter(new KafkaNet.Model.KafkaOptions
            {
                KafkaServerUri = new List<Uri> { new Uri("http://localhost:1") },
                KafkaConnectionFactory = _factoryMock.Object
            });

            Assert.That(result, Is.Not.Null);
        }

        [Test]
        public void BrokerRouterUsesFactoryToAddNewBrokers()
        {
            var router = new BrokerRouter(new KafkaNet.Model.KafkaOptions
            {
                KafkaServerUri = new List<Uri> { new Uri("http://localhost:1") },
                KafkaConnectionFactory = _factoryMock.Object
            });

            _connMock1.Setup(x => x.SendAsync(It.IsAny<IKafkaRequest<MetadataResponse>>()))
                      .Returns(() => Task.Factory.StartNew(() => new List<MetadataResponse> { CreateMetaResponse() }));

            var topics = router.GetTopicMetadata(TestTopic);
            _factoryMock.Verify(x => x.Create(It.Is<Uri>(uri => uri.Port == 2), It.IsAny<int>(), It.IsAny<IKafkaLog>()), Times.Once());
        }

        #region MetadataRequest Tests...
        [Test]
        public void BrokerRouteShouldCycleThroughEachBrokerUntilOneIsFound()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy.BrokerConn0.MetadataResponseFunction = () => { throw new Exception("some error"); };
            var router = routerProxy.Create();

            var result = router.GetTopicMetadata(TestTopic);
            Assert.That(result, Is.Not.Null);
            Assert.That(routerProxy.BrokerConn0.MetadataRequestCallCount, Is.EqualTo(1));
            Assert.That(routerProxy.BrokerConn1.MetadataRequestCallCount, Is.EqualTo(1));
        }

        [Test]
        [ExpectedException(typeof(ServerUnreachableException))]
        public void BrokerRouteShouldThrowIfCycleCouldNotConnectToAnyServer()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy.BrokerConn0.MetadataResponseFunction = () => { throw new Exception("some error"); };
            routerProxy.BrokerConn1.MetadataResponseFunction = () => { throw new Exception("some error"); };
            var router = routerProxy.Create();

            try
            {
                router.GetTopicMetadata(TestTopic);
            }
            catch
            {
                Assert.That(routerProxy.BrokerConn0.MetadataRequestCallCount, Is.EqualTo(1));
                Assert.That(routerProxy.BrokerConn1.MetadataRequestCallCount, Is.EqualTo(1));
                throw;
            }
        }

        [Test]
        public void BrokerRouteShouldReturnTopicFromCache()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            var router = routerProxy.Create();

            var result1 = router.GetTopicMetadata(TestTopic);
            var result2 = router.GetTopicMetadata(TestTopic);

            Assert.That(routerProxy.BrokerConn0.MetadataRequestCallCount, Is.EqualTo(1));
            Assert.That(result1.Count, Is.EqualTo(1));
            Assert.That(result1[0].Name, Is.EqualTo(TestTopic));
            Assert.That(result2.Count, Is.EqualTo(1));
            Assert.That(result2[0].Name, Is.EqualTo(TestTopic));
        }

        [Test]
        public void RefreshTopicMetadataShouldIgnoreCacheAndAlwayCauseMetadataRequest()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            var router = routerProxy.Create();

            router.RefreshTopicMetadata(new[] { TestTopic });
            Assert.That(routerProxy.BrokerConn0.MetadataRequestCallCount, Is.EqualTo(1));

            router.RefreshTopicMetadata(new[] { TestTopic });
            Assert.That(routerProxy.BrokerConn0.MetadataRequestCallCount, Is.EqualTo(2));
        }
        #endregion

        #region SelectBrokerRouteAsync Exact Tests...
        [Test]
        public void SelectExactPartitionShouldReturnRequestedPartition()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            var router = routerProxy.Create();

            var p0 = router.SelectBrokerRoute(TestTopic, 0);
            var p1 = router.SelectBrokerRoute(TestTopic, 1);

            Assert.That(p0.PartitionId, Is.EqualTo(0));
            Assert.That(p1.PartitionId, Is.EqualTo(1));
        }

        [Test]
        [ExpectedException(typeof(InvalidPartitionException))]
        public void SelectExactPartitionShouldThrowWhenPartitionDoesNotExist()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);

            routerProxy.Create().SelectBrokerRoute(TestTopic, 3);
        }

        [Test]
        [ExpectedException(typeof(InvalidTopicMetadataException))]
        public void SelectExactPartitionShouldThrowWhenTopicsCollectionIsEmpty()
        {
            var metadataResponse = CreateMetaResponse();
            metadataResponse.Topics.Clear();

            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy.BrokerConn0.MetadataResponseFunction = () => metadataResponse;

            routerProxy.Create().SelectBrokerRoute(TestTopic, 1);
        }

        [Test]
        [ExpectedException(typeof(LeaderNotFoundException))]
        public void SelectExactPartitionShouldThrowWhenBrokerCollectionIsEmpty()
        {
            var metadataResponse = CreateMetaResponse();
            metadataResponse.Brokers.Clear();

            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy.BrokerConn0.MetadataResponseFunction = () => metadataResponse;


            routerProxy.Create().SelectBrokerRoute(TestTopic, 1);
        }
        #endregion

        #region SelectBrokerRouteAsync Select Tests...

        [Test]
        [TestCase(null)]
        [TestCase("withkey")]
        public void SelectPartitionShouldUsePartitionSelector(string key)
        {
            var routerProxy = new BrokerRouterProxy(_kernel);

            _partitionSelectorMock.Setup(x => x.Select(It.IsAny<Topic>(), key))
                                  .Returns(() => new Partition
                                  {
                                      ErrorCode = 0,
                                      Isrs = new List<int> { 1 },
                                      PartitionId = 0,
                                      LeaderId = 0,
                                      Replicas = new List<int> { 1 },
                                  });

            routerProxy.PartitionSelector = _partitionSelectorMock.Object;

            var result = routerProxy.Create().SelectBrokerRoute(TestTopic, key);

            _partitionSelectorMock.Verify(f => f.Select(It.Is<Topic>(x => x.Name == TestTopic), key), Times.Once());
        }

        [Test]
        [ExpectedException(typeof(InvalidTopicMetadataException))]
        public void SelectPartitionShouldThrowWhenTopicsCollectionIsEmpty()
        {
            var metadataResponse = CreateMetaResponse();
            metadataResponse.Topics.Clear();


            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy.BrokerConn0.MetadataResponseFunction = () => metadataResponse;

            routerProxy.Create().SelectBrokerRoute(TestTopic);
        }

        [Test]
        [ExpectedException(typeof(LeaderNotFoundException))]
        public void SelectPartitionShouldThrowWhenBrokerCollectionIsEmpty()
        {
            var metadataResponse = CreateMetaResponse();
            metadataResponse.Brokers.Clear();

            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy.BrokerConn0.MetadataResponseFunction = () => metadataResponse;

            routerProxy.Create().SelectBrokerRoute(TestTopic);
        }
        #endregion

        #region Private Methods...
        private MetadataResponse CreateMetaResponse()
        {
            return new MetadataResponse
                {
                    CorrelationId = 1,
                    Brokers = new List<Broker>
                        {
                            new Broker
                                {
                                    Host = "localhost",
                                    Port = 1,
                                    BrokerId = 0
                                },
                            new Broker
                                {
                                    Host = "localhost",
                                    Port = 2,
                                    BrokerId = 1
                                },
                        },
                    Topics = new List<Topic>
                        {
                            new Topic
                                {
                                    ErrorCode = 0,
                                    Name = TestTopic,
                                    Partitions = new List<Partition>
                                        {
                                            new Partition
                                                {
                                                    ErrorCode = 0,
                                                    Isrs = new List<int> {1},
                                                    PartitionId = 0,
                                                    LeaderId = 0,
                                                    Replicas = new List<int> {1},
                                                },
                                            new Partition
                                                {
                                                    ErrorCode = 0,
                                                    Isrs = new List<int> {1},
                                                    PartitionId = 1,
                                                    LeaderId = 1,
                                                    Replicas = new List<int> {1},
                                                }
                                        }

                                }
                        }
                };
        }
        #endregion
    }
}
