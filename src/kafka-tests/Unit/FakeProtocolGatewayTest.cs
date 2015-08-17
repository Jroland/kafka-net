using kafka_tests.Helpers;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using Moq;
using Ninject.MockingKernel.Moq;
using NUnit.Framework;
using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace kafka_tests.Unit
{
    [TestFixture]
    [Category("unit")]
    public class FakeProtocolGatewayTest
    {
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

        [TestCase(ErrorResponseCode.NotLeaderForPartition)]
        [TestCase(ErrorResponseCode.LeaderNotAvailable)]
        [TestCase(ErrorResponseCode.ConsumerCoordinatorNotAvailableCode)]
        [TestCase(ErrorResponseCode.BrokerNotAvailable)]
        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task ShouldTryToRefreshMataDataIfCanRecoverByRefreshMetadata(ErrorResponseCode code)
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy._cacheExpiration = new TimeSpan(10);
            var router = routerProxy.Create();

            int partitionId = 0;

            ProtocolGateway protocolGateway = new ProtocolGateway(router);
            var fetchRequest = new FetchRequest();
            bool sendExOnFirstTime = true;

            Func<Task<FetchResponse>> ShouldReturnErrorAndThenNoError = async () =>
            {
                Task.Delay(routerProxy._cacheExpiration).Wait();
                Task.Delay(1).Wait();
                if (sendExOnFirstTime)
                {
                    sendExOnFirstTime = false;
                    return new FetchResponse() { Error = (short)code };
                }
                return new FetchResponse() { Error = (short)ErrorResponseCode.NoError };
            };
            routerProxy.BrokerConn0.FetchResponseFunction = ShouldReturnErrorAndThenNoError;
            routerProxy.BrokerConn0.MetadataResponseFunction = BrokerRouterProxy.DefaultMetadataResponse;

            await protocolGateway.SendProtocolRequest(fetchRequest, BrokerRouterProxy.TestTopic, partitionId);

            Assert.That(routerProxy.BrokerConn0.MetadataRequestCallCount, Is.EqualTo(2));
            Assert.That(routerProxy.BrokerConn0.FetchRequestCallCount, Is.EqualTo(2));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task ShouldTryToRefreshMataDataIfSocketException()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy._cacheExpiration = TimeSpan.FromMilliseconds(10);
            var router = routerProxy.Create();

            int partitionId = 0;

            ProtocolGateway protocolGateway = new ProtocolGateway(router);
            var fetchRequest = new FetchRequest();
            bool sendExOnFirstTime = true;

            Func<Task<FetchResponse>> ShouldReturnNotLeaderForPartitionAndThenNoError = async () =>
            {
                Task.Delay(routerProxy._cacheExpiration).Wait();
                Task.Delay(1).Wait();
                if (sendExOnFirstTime)
                {
                    sendExOnFirstTime = false;
                    throw new SocketException();
                }
                return new FetchResponse() { Error = (short)ErrorResponseCode.NoError };
            };
            routerProxy.BrokerConn0.FetchResponseFunction = ShouldReturnNotLeaderForPartitionAndThenNoError;
            routerProxy.BrokerConn0.MetadataResponseFunction = BrokerRouterProxy.DefaultMetadataResponse;

            await protocolGateway.SendProtocolRequest(fetchRequest, BrokerRouterProxy.TestTopic, partitionId);

            Assert.That(routerProxy.BrokerConn0.MetadataRequestCallCount, Is.EqualTo(2));
            Assert.That(routerProxy.BrokerConn0.FetchRequestCallCount, Is.EqualTo(2));
        }

        [TestCase(typeof(Exception))]
        [TestCase(typeof(KafkaApplicationException))]
        public async Task SendProtocolRequestShouldThrowException(Type exceptionType)
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy._cacheExpiration = TimeSpan.FromMilliseconds(10);
            var router = routerProxy.Create();

            int partitionId = 0;
            ProtocolGateway protocolGateway = new ProtocolGateway(router);
            var fetchRequest = new FetchRequest();

            bool firstTime = true;
            Func<Task<FetchResponse>> ShouldReturnError = async () =>
            {
                if (firstTime)
                {
                    firstTime = !firstTime;
                    object[] args = new object[1];
                    args[0] = "error Test";
                    throw (Exception)Activator.CreateInstance(exceptionType, args);
                }
                return new FetchResponse() { Error = (short)ErrorResponseCode.NoError };
            };

            routerProxy.BrokerConn0.FetchResponseFunction = ShouldReturnError;
            routerProxy.BrokerConn0.MetadataResponseFunction = BrokerRouterProxy.DefaultMetadataResponse;
            try
            {
                await protocolGateway.SendProtocolRequest(fetchRequest, BrokerRouterProxy.TestTopic, partitionId);
                Assert.IsTrue(false, "Should throw exception");
            }
            catch (Exception ex)
            {
                Assert.That(ex.GetType(), Is.EqualTo(exceptionType));
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [ExpectedException(typeof(KafkaApplicationException))]
        [TestCase(ErrorResponseCode.InvalidMessage)]
        [TestCase(ErrorResponseCode.InvalidMessageSize)]
        [TestCase(ErrorResponseCode.MessageSizeTooLarge)]
        [TestCase(ErrorResponseCode.OffsetMetadataTooLargeCode)]
        [TestCase(ErrorResponseCode.OffsetOutOfRange)]
        [TestCase(ErrorResponseCode.NotCoordinatorForConsumerCode)]
        [TestCase(ErrorResponseCode.RequestTimedOut)]
        [TestCase(ErrorResponseCode.OffsetsLoadInProgressCode)]
        [TestCase(ErrorResponseCode.UnknownTopicOrPartition)]
        [TestCase(ErrorResponseCode.Unknown)]
        [TestCase(ErrorResponseCode.StaleControllerEpochCode)]
        [TestCase(ErrorResponseCode.ReplicaNotAvailable)]
        public async Task SendProtocolRequestShouldNoTryToRefreshMataDataIfCanNotRecoverByRefreshMetadata(ErrorResponseCode code)
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy._cacheExpiration = TimeSpan.FromMilliseconds(10);
            var router = routerProxy.Create();

            int partitionId = 0;
            ProtocolGateway protocolGateway = new ProtocolGateway(router);
            var fetchRequest = new FetchRequest();

            bool firstTime = true;
            Func<Task<FetchResponse>> ShouldReturnError = async () =>
            {
                if (firstTime)
                {
                    firstTime = !firstTime;
                    return new FetchResponse() { Error = (short)code };
                }
                return new FetchResponse() { Error = (short)ErrorResponseCode.NoError };
            };
            routerProxy.BrokerConn0.FetchResponseFunction = ShouldReturnError;
            routerProxy.BrokerConn0.MetadataResponseFunction = BrokerRouterProxy.DefaultMetadataResponse;
            await protocolGateway.SendProtocolRequest(fetchRequest, BrokerRouterProxy.TestTopic, partitionId);
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task ShouldUpdateMetadataOnes()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy._cacheExpiration = TimeSpan.FromMilliseconds(10);
            var router = routerProxy.Create();

            int partitionId = 0;
            ProtocolGateway protocolGateway = new ProtocolGateway(router);
            var fetchRequest = new FetchRequest();

            bool firstTime = true;
            Func<Task<FetchResponse>> ShouldReturnError = async () =>
            {
                return new FetchResponse() { Error = (short)ErrorResponseCode.NoError };
            };

            routerProxy.BrokerConn0.FetchResponseFunction = ShouldReturnError;
            routerProxy.BrokerConn0.MetadataResponseFunction = BrokerRouterProxy.DefaultMetadataResponse;
            int numberOfCall = 1000;
            Task[] tasks = new Task[numberOfCall];
            for (int i = 0; i < numberOfCall / 2; i++)
            {
                tasks[i] = protocolGateway.SendProtocolRequest(fetchRequest, BrokerRouterProxy.TestTopic, partitionId);
            }
            await Task.Delay(routerProxy._cacheExpiration);
            await Task.Delay(1);
            for (int i = 0; i < numberOfCall / 2; i++)
            {
                tasks[i + numberOfCall / 2] = protocolGateway.SendProtocolRequest(fetchRequest, BrokerRouterProxy.TestTopic, partitionId);
            }

            await Task.WhenAll(tasks);
            Assert.That(routerProxy.BrokerConn0.FetchRequestCallCount, Is.EqualTo(numberOfCall));
            Assert.That(routerProxy.BrokerConn0.MetadataRequestCallCount, Is.EqualTo(1));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task ShouldRecoverUpdateMetadataForNewTopic()
        {
            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy._cacheExpiration = TimeSpan.FromMilliseconds(10);
            var router = routerProxy.Create();

            int partitionId = 0;
            ProtocolGateway protocolGateway = new ProtocolGateway(router);
            var fetchRequest = new FetchRequest();

            bool firstTime = true;
            Func<Task<FetchResponse>> ShouldReturnError = async () =>
             {
                 return new FetchResponse() { Error = (short)ErrorResponseCode.NoError };
             };

            routerProxy.BrokerConn0.FetchResponseFunction = ShouldReturnError;
            routerProxy.BrokerConn0.MetadataResponseFunction = BrokerRouterProxy.DefaultMetadataResponse;
            int numberOfCall = 1000;
            Task[] tasks = new Task[numberOfCall];
            for (int i = 0; i < numberOfCall / 2; i++)
            {
                tasks[i] = protocolGateway.SendProtocolRequest(fetchRequest, BrokerRouterProxy.TestTopic, partitionId);
            }
            //change MetadataResponseFunction
            routerProxy.BrokerConn0.MetadataResponseFunction = async () =>
            {
                var response = await BrokerRouterProxy.DefaultMetadataResponse();
                response.Topics[0].Name = "test2";
                return response;
            };

            for (int i = 0; i < numberOfCall / 2; i++)
            {
                tasks[i + numberOfCall / 2] = protocolGateway.SendProtocolRequest(fetchRequest, "test2", partitionId);
            }

            await Task.WhenAll(tasks);
            Assert.That(routerProxy.BrokerConn0.FetchRequestCallCount, Is.EqualTo(numberOfCall));
            Assert.That(routerProxy.BrokerConn0.MetadataRequestCallCount, Is.EqualTo(2));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task ShouldRecoverFromFailerByUpdateMetadataOnce()//Do not debug this test !!
        {
            var log = new DefaultTraceLog();
            var routerProxy = new BrokerRouterProxy(_kernel);
            routerProxy._cacheExpiration = TimeSpan.FromMilliseconds(1000);
            var router = routerProxy.Create();

            int partitionId = 0;
            ProtocolGateway protocolGateway = new ProtocolGateway(router);
            var fetchRequest = new FetchRequest();

            int numberOfCall = 100;
            long numberOfErrorSend = 0;
            TaskCompletionSource<int> x = new TaskCompletionSource<int>();
            Func<Task<FetchResponse>> ShouldReturnNotLeaderForPartitionAndThenNoError = async () =>
            {
                log.DebugFormat("FetchResponse Start ");
                if (!x.Task.IsCompleted)
                {
                    if (Interlocked.Increment(ref numberOfErrorSend) == numberOfCall)
                    {
                        await Task.Delay(routerProxy._cacheExpiration);
                        await Task.Delay(1);
                        x.TrySetResult(1);
                        log.DebugFormat("all is complete ");
                    }

                    await x.Task;
                    log.DebugFormat("SocketException ");
                    throw new SocketException();
                }
                log.DebugFormat("Completed ");

                return new FetchResponse() { Error = (short)ErrorResponseCode.NoError };
            };

            routerProxy.BrokerConn0.FetchResponseFunction = ShouldReturnNotLeaderForPartitionAndThenNoError;
            routerProxy.BrokerConn0.MetadataResponseFunction = BrokerRouterProxy.DefaultMetadataResponse;

            Task[] tasks = new Task[numberOfCall];

            for (int i = 0; i < numberOfCall; i++)
            {
                tasks[i] = protocolGateway.SendProtocolRequest(fetchRequest, BrokerRouterProxy.TestTopic, partitionId);
            }

            await Task.WhenAll(tasks);
            Assert.That(numberOfErrorSend, Is.GreaterThan(1), "numberOfErrorSend");
            Assert.That(routerProxy.BrokerConn0.FetchRequestCallCount, Is.EqualTo(numberOfCall + numberOfErrorSend), "FetchRequestCallCount");
            Assert.That(routerProxy.BrokerConn0.MetadataRequestCallCount, Is.EqualTo(2), "MetadataRequestCallCount");
        }
    }
}