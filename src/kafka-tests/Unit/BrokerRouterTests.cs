﻿using kafka_tests.Helpers;
using KafkaNet;
using KafkaNet.Common;
using KafkaNet.Model;
using KafkaNet.Protocol;
using Moq;
using Ninject.MockingKernel.Moq;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Net;
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

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void BrokerRouterCanConstruct()
        {
            var result = new BrokerRouter(new KafkaOptions
            {
                KafkaServerUri = new List<Uri> { new Uri("http://localhost:1") },
                KafkaConnectionFactory = _mockKafkaConnectionFactory.Object
            });

            Assert.That(result, Is.Not.Null);
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [ExpectedException(typeof(ServerUnreachableException))]
        public void BrokerRouterConstructorThrowsServerUnreachableException()
        {
            var result = new BrokerRouter(new KafkaOptions
            {
                KafkaServerUri = new List<Uri> { new Uri("http://noaddress:1") }
            });
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void BrokerRouterConstructorShouldIgnoreUnresolvableUriWhenAtLeastOneIsGood()
        {
            var result = new BrokerRouter(new KafkaOptions
            {
                KafkaServerUri = new List<Uri> { new Uri("http://noaddress:1"), new Uri("http://localhost:1") }
            });
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task BrokerRouterUsesFactoryToAddNewBrokers()
        {
            var router = new BrokerRouter(new KafkaOptions
            {
                KafkaServerUri = new List<Uri> { new Uri("http://localhost:1") },
                KafkaConnectionFactory = _mockKafkaConnectionFactory.Object
            });

            _mockKafkaConnection1.Setup(x => x.SendAsync(It.IsAny<IKafkaRequest<MetadataResponse>>()))
                      .Returns(() => Task.Run(() => new List<MetadataResponse> { CreateMetaResponse() }));
            await router.RefreshMissingTopicMetadata(TestTopic);
            var topics = router.GetTopicMetadataFromLocalCache(TestTopic);
            _mockKafkaConnectionFactory.Verify(x => x.Create(It.Is<KafkaEndpoint>(e => e.Endpoint.Port == 2), It.IsAny<TimeSpan>(), It.IsAny<IKafkaLog>(), null), Times.Once());
        }

        #region MetadataRequest Tests...

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task BrokerRouteShouldCycleThroughEachBrokerUntilOneIsFound()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy.BrokerConn0.MetadataResponseFunction = () => { throw new Exception("some error"); };
            var router = routerProxy.Create();
            await router.RefreshMissingTopicMetadata(TestTopic);
            var result = router.GetTopicMetadataFromLocalCache(TestTopic);
            Assert.That(result, Is.Not.Null);
            Assert.That(routerProxy.BrokerConn0.MetadataRequestCallCount, Is.EqualTo(1));
            Assert.That(routerProxy.BrokerConn1.MetadataRequestCallCount, Is.EqualTo(1));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [ExpectedException(typeof(ServerUnreachableException))]
        public async Task BrokerRouteShouldThrowIfCycleCouldNotConnectToAnyServer()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy.BrokerConn0.MetadataResponseFunction = () => { throw new Exception("some error"); };
            routerProxy.BrokerConn1.MetadataResponseFunction = () => { throw new Exception("some error"); };
            var router = routerProxy.Create();

            try
            {
                await router.RefreshMissingTopicMetadata(TestTopic);
                router.GetTopicMetadataFromLocalCache(TestTopic);
            }
            catch
            {
                Assert.That(routerProxy.BrokerConn0.MetadataRequestCallCount, Is.EqualTo(1));
                Assert.That(routerProxy.BrokerConn1.MetadataRequestCallCount, Is.EqualTo(1));
                throw;
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task BrokerRouteShouldReturnTopicFromCache()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            var router = routerProxy.Create();
            await router.RefreshMissingTopicMetadata(TestTopic);
            var result1 = router.GetTopicMetadataFromLocalCache(TestTopic);
            var result2 = router.GetTopicMetadataFromLocalCache(TestTopic);

            Assert.That(routerProxy.BrokerConn0.MetadataRequestCallCount, Is.EqualTo(1));
            Assert.That(result1.Count, Is.EqualTo(1));
            Assert.That(result1[0].Name, Is.EqualTo(TestTopic));
            Assert.That(result2.Count, Is.EqualTo(1));
            Assert.That(result2[0].Name, Is.EqualTo(TestTopic));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task RefreshTopicMetadataShouldIgnoreCacheAndAlwayCauseMetadataRequestAfterExpertionDate()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            var router = routerProxy.Create();
            TimeSpan cacheExpiration = TimeSpan.FromMilliseconds(100);
            await router.RefreshTopicMetadata(TestTopic);
            Assert.That(routerProxy.BrokerConn0.MetadataRequestCallCount, Is.EqualTo(1));
            await Task.Delay(routerProxy._cacheExpiration);
            await Task.Delay(1);//After cache is expair
            await router.RefreshTopicMetadata(TestTopic);
            Assert.That(routerProxy.BrokerConn0.MetadataRequestCallCount, Is.EqualTo(2));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task SimultaneouslyRefreshTopicMetadataShouldGetDataFromCacheOnSameRequest()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            var router = routerProxy.Create();

            List<Task> x = new List<Task>();
            x.Add(router.RefreshTopicMetadata(TestTopic));//do not debug
            x.Add(router.RefreshTopicMetadata(TestTopic));//do not debug
            await Task.WhenAll(x.ToArray());
            Assert.That(routerProxy.BrokerConn0.MetadataRequestCallCount, Is.EqualTo(1));
        }

        #endregion MetadataRequest Tests...

        #region SelectBrokerRouteAsync Exact Tests...

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task SelectExactPartitionShouldReturnRequestedPartition()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            var router = routerProxy.Create();
            await router.RefreshMissingTopicMetadata(TestTopic);
            var p0 = router.SelectBrokerRouteFromLocalCache(TestTopic, 0);
            var p1 = router.SelectBrokerRouteFromLocalCache(TestTopic, 1);

            Assert.That(p0.PartitionId, Is.EqualTo(0));
            Assert.That(p1.PartitionId, Is.EqualTo(1));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [ExpectedException(typeof(InvalidPartitionException))]
        public async Task SelectExactPartitionShouldThrowWhenPartitionDoesNotExist()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            var router = routerProxy.Create();
            await router.RefreshMissingTopicMetadata(TestTopic);
            router.SelectBrokerRouteFromLocalCache(TestTopic, 3);
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [ExpectedException(typeof(InvalidTopicNotExistsInCache))]
        public void SelectExactPartitionShouldThrowWhenTopicsCollectionIsEmpty()
        {
            var metadataResponse = CreateMetaResponse();
            metadataResponse.Topics.Clear();

            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy.BrokerConn0.MetadataResponseFunction = async () => metadataResponse;

            routerProxy.Create().SelectBrokerRouteFromLocalCache(TestTopic, 1);
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [ExpectedException(typeof(LeaderNotFoundException))]
        public async Task SelectExactPartitionShouldThrowWhenBrokerCollectionIsEmpty()
        {
            var metadataResponse = CreateMetaResponse();
            metadataResponse.Brokers.Clear();

            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy.BrokerConn0.MetadataResponseFunction = async () => metadataResponse;
            var router = routerProxy.Create();
            await router.RefreshMissingTopicMetadata(TestTopic);
            router.SelectBrokerRouteFromLocalCache(TestTopic, 1);
        }

        #endregion SelectBrokerRouteAsync Exact Tests...

        #region SelectBrokerRouteAsync Select Tests...

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [TestCase(null)]
        [TestCase("withkey")]
        public async Task SelectPartitionShouldUsePartitionSelector(string testCase)
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
            var router = routerProxy.Create();
            await router.RefreshMissingTopicMetadata(TestTopic);
            var result = router.SelectBrokerRouteFromLocalCache(TestTopic, key);

            _mockPartitionSelector.Verify(f => f.Select(It.Is<Topic>(x => x.Name == TestTopic), key), Times.Once());
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [ExpectedException(typeof(InvalidTopicNotExistsInCache))]
        public void SelectPartitionShouldThrowWhenTopicsCollectionIsEmpty()
        {
            var metadataResponse = CreateMetaResponse();
            metadataResponse.Topics.Clear();

            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy.BrokerConn0.MetadataResponseFunction = async () => metadataResponse;

            routerProxy.Create().SelectBrokerRouteFromLocalCache(TestTopic);
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [ExpectedException(typeof(LeaderNotFoundException))]
        public async Task SelectPartitionShouldThrowWhenBrokerCollectionIsEmpty()
        {
            var metadataResponse = CreateMetaResponse();
            metadataResponse.Brokers.Clear();

            var routerProxy = new BrokerRouterProxy(_kernel);
            var router = routerProxy.BrokerConn0;
            routerProxy.BrokerConn0.MetadataResponseFunction = async () => metadataResponse;
            var routerProxy1 = routerProxy.Create();
            await routerProxy1.RefreshMissingTopicMetadata(TestTopic);
            routerProxy1.SelectBrokerRouteFromLocalCache(TestTopic);
        }

        #endregion SelectBrokerRouteAsync Select Tests...

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

        #endregion Private Methods...
    }
}