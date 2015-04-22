using System.Net;
using System.Runtime.Remoting.Messaging;
using KafkaNet;
using KafkaNet.Common;
using KafkaNet.Model;
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
        private Mock<IKafkaConnection> _mockKafkaConnection1;
        private Mock<IKafkaConnectionFactory> _mockKafkaConnectionFactory;
        private Mock<IPartitionSelector> _mockPartitionSelector;

        [SetUp]
        public void Setup()
        {
            _kernel = new MoqMockingKernel();

            //setup mock IKafkaConnection
            _mockPartitionSelector = _kernel.GetMock<IPartitionSelector>();
            _mockKafkaConnection1 = _kernel.GetMock<IKafkaConnection>();
            _mockKafkaConnectionFactory = _kernel.GetMock<IKafkaConnectionFactory>();
            _mockKafkaConnectionFactory.Setup(x => x.Create(It.Is<KafkaEndpoint>(e => e.Endpoint.Port == 1), It.IsAny<TimeSpan>(), It.IsAny<IKafkaLog>(), null)).Returns(() => _mockKafkaConnection1.Object);
            _mockKafkaConnectionFactory.Setup(x => x.Resolve(It.IsAny<Uri>(), It.IsAny<IKafkaLog>()))
                .Returns<Uri, IKafkaLog>((uri, log) => new KafkaEndpoint
                {
                    Endpoint = new IPEndPoint(IPAddress.Parse("127.0.0.1"), uri.Port),
                    ServeUri = uri
                });
        }

        [Test]
        public void BrokerRouterCanConstruct()
        {
            var result = new BrokerRouter(new KafkaOptions
            {
                KafkaServerUri = new List<Uri> { new Uri("http://localhost:1") },
                KafkaConnectionFactory = _mockKafkaConnectionFactory.Object
            });

            Assert.That(result, Is.Not.Null);
        }

        [Test]
        [ExpectedException(typeof(ServerUnreachableException))]
        public void BrokerRouterConstructorThrowsServerUnreachableException()
        {
            var result = new BrokerRouter(new KafkaOptions
            {
                KafkaServerUri = new List<Uri> { new Uri("http://noaddress:1") }
            });
        }

        [Test]
        public void BrokerRouterConstructorShouldIgnoreUnresolvableUriWhenAtLeastOneIsGood()
        {
            var result = new BrokerRouter(new KafkaOptions
            {
                KafkaServerUri = new List<Uri> { new Uri("http://noaddress:1"), new Uri("http://localhost:1") }
            });
        }

        [Test]
        public void BrokerRouterUsesFactoryToAddNewBrokers()
        {
            var router = new BrokerRouter(new KafkaOptions
            {
                KafkaServerUri = new List<Uri> { new Uri("http://localhost:1") },
                KafkaConnectionFactory = _mockKafkaConnectionFactory.Object
            });

            _mockKafkaConnection1.Setup(x => x.SendAsync(It.IsAny<IKafkaRequest<MetadataResponse>>()))
                      .Returns(() => Task.Run(() => new List<MetadataResponse> { CreateMetaResponse() }));

            var topics = router.GetTopicMetadata(TestTopic);
            _mockKafkaConnectionFactory.Verify(x => x.Create(It.Is<KafkaEndpoint>(e => e.Endpoint.Port == 2), It.IsAny<TimeSpan>(), It.IsAny<IKafkaLog>(), null), Times.Once());
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

            router.RefreshTopicMetadata(TestTopic);
            Assert.That(routerProxy.BrokerConn0.MetadataRequestCallCount, Is.EqualTo(1));

            router.RefreshTopicMetadata(TestTopic);
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
        public void SelectPartitionShouldUsePartitionSelector(string testCase)
        {
            var key = testCase.ToIntSizedBytes();
            var routerProxy = new BrokerRouterProxy(_kernel);

            _mockPartitionSelector.Setup(x => x.Select(It.IsAny<Topic>(), key))
                                  .Returns(() => new Partition
                                  {
                                      ErrorCode = 0,
                                      Isrs = new List<int> { 1 },
                                      PartitionId = 0,
                                      LeaderId = 0,
                                      Replicas = new List<int> { 1 },
                                  });

            routerProxy.PartitionSelector = _mockPartitionSelector.Object;

            var result = routerProxy.Create().SelectBrokerRoute(TestTopic, key);

            _mockPartitionSelector.Verify(f => f.Select(It.Is<Topic>(x => x.Name == TestTopic), key), Times.Once());
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
