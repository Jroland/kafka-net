using kafka_tests.Fakes;
using kafka_tests.Helpers;
using KafkaNet;
using KafkaNet.Common;
using KafkaNet.Model;
using KafkaNet.Protocol;
using Moq;
using Ninject.MockingKernel.Moq;
using NUnit.Framework;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace kafka_tests.Unit
{
    [TestFixture]
    public class KafkaConnectionTests
    {
        private readonly DefaultTraceLog _log;
        private readonly KafkaEndpoint _kafkaEndpoint;
        private MoqMockingKernel _kernel;

        public KafkaConnectionTests()
        {
            _log = new DefaultTraceLog();
            _kafkaEndpoint = new DefaultKafkaConnectionFactory().Resolve(new Uri("http://localhost:8999"), _log);
        }

        [SetUp]
        public void Setup()
        {
            _kernel = new MoqMockingKernel();
        }

        #region Construct...

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task ShouldStartReadPollingOnConstruction()
        {
            using (var socket = new KafkaTcpSocket(_log, _kafkaEndpoint))
            using (var conn = new KafkaConnection(socket, log: _log))
            {
                await TaskTest.WaitFor(() => conn.ReadPolling);
                Assert.That(conn.ReadPolling, Is.True);
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void ShouldReportServerUriOnConstruction()
        {
            var expectedUrl = _kafkaEndpoint;
            using (var socket = new KafkaTcpSocket(_log, expectedUrl))
            using (var conn = new KafkaConnection(socket, log: _log))
            {
                Assert.That(conn.Endpoint, Is.EqualTo(expectedUrl));
            }
        }

        #endregion Construct...

        #region Dispose Tests...

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task ShouldDisposeWithoutExceptionThrown()
        {
            using (var server = new FakeTcpServer(_log, 8999))
            using (var socket = new KafkaTcpSocket(_log, _kafkaEndpoint))
            {
                var conn = new KafkaConnection(socket, log: _log);
                await TaskTest.WaitFor(() => server.ConnectionEventcount > 0);
                using (conn) { }
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task ShouldDisposeWithoutExceptionEvenWhileCallingSendAsync()
        {
            using (var socket = new KafkaTcpSocket(_log, _kafkaEndpoint))
            using (var conn = new KafkaConnection(socket, log: _log))
            {
                var task = conn.SendAsync(new MetadataRequest());
                task.Wait(TimeSpan.FromMilliseconds(1000));
                Assert.That(task.IsCompleted, Is.False, "The send task should still be pending.");
            }
        }

        #endregion Dispose Tests...

        #region Read Tests...

        //TODO FIx has moc error
        [Test, Repeat(1)]
        public async Task ReadShouldLogDisconnectAndRecover()
        {
            var mockLog = _kernel.GetMock<IKafkaLog>();

            using (var server = new FakeTcpServer(_log, 8999))
            using (var socket = new KafkaTcpSocket(mockLog.Object, _kafkaEndpoint))
            using (var conn = new KafkaConnection(socket, log: mockLog.Object))
            {
                var disconnected = false;
                socket.OnServerDisconnected += () => disconnected = true;

                await TaskTest.WaitFor(() => server.ConnectionEventcount > 0);
                Assert.That(server.ConnectionEventcount, Is.EqualTo(1));

                server.DropConnection();
                await TaskTest.WaitFor(() => server.DisconnectionEventCount > 0);
                Assert.That(server.DisconnectionEventCount, Is.EqualTo(1));

                //Wait a while for the client to notice the disconnect and log
                await TaskTest.WaitFor(() => disconnected);

                //should log an exception and keep going
                mockLog.Verify(x => x.ErrorFormat(It.IsAny<string>(), It.IsAny<Exception>()));

                await TaskTest.WaitFor(() => server.ConnectionEventcount > 1);
                Assert.That(server.ConnectionEventcount, Is.EqualTo(2));
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task ReadShouldIgnoreMessageWithUnknownCorrelationId()
        {
            const int correlationId = 99;

            var mockLog = _kernel.GetMock<IKafkaLog>();

            using (var server = new FakeTcpServer(_log, 8999))
            using (var socket = new KafkaTcpSocket(mockLog.Object, _kafkaEndpoint))
            using (var conn = new KafkaConnection(socket, log: mockLog.Object))
            {
                var receivedData = false;
                socket.OnBytesReceived += i => receivedData = true;

                //send correlation message
                server.SendDataAsync(CreateCorrelationMessage(correlationId)).Wait(TimeSpan.FromSeconds(5));

                //wait for connection
                await TaskTest.WaitFor(() => server.ConnectionEventcount > 0);
                Assert.That(server.ConnectionEventcount, Is.EqualTo(1));

                await TaskTest.WaitFor(() => receivedData);

                //should log a warning and keep going
                mockLog.Verify(x => x.WarnFormat(It.IsAny<string>(), It.Is<int>(o => o == correlationId)));
            }
        }

        #endregion Read Tests...

        #region Send Tests...

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task SendAsyncShouldTimeoutWhenSendAsyncTakesTooLong()
        {
            using (var server = new FakeTcpServer(_log, 8999))
            using (var socket = new KafkaTcpSocket(_log, _kafkaEndpoint))
            using (var conn = new KafkaConnection(socket, TimeSpan.FromMilliseconds(1), log: _log))
            {
                await TaskTest.WaitFor(() => server.ConnectionEventcount > 0);
                Assert.That(server.ConnectionEventcount, Is.EqualTo(1));

                var taskResult = conn.SendAsync(new MetadataRequest());

                taskResult.ContinueWith(t => taskResult = t).Wait(TimeSpan.FromMilliseconds(100));

                Assert.That(taskResult.IsFaulted, Is.True, "Task should have reported an exception.");
                Assert.That(taskResult.Exception.InnerException, Is.TypeOf<ResponseTimeoutException>());
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async void SendAsyncShouldNotAllowResponseToTimeoutWhileAwaitingKafkaToEstableConnection()
        {
            using (var socket = new KafkaTcpSocket(_log, _kafkaEndpoint))
            using (var conn = new KafkaConnection(socket, TimeSpan.FromMilliseconds(1000000), log: _log))
            {
                Console.WriteLine("SendAsync blocked by reconnection attempts...");
                var taskResult = conn.SendAsync(new MetadataRequest());

                taskResult.ContinueWith(t => taskResult = t).Wait(TimeSpan.FromSeconds(1));

                Console.WriteLine("Task result should be WaitingForActivation...");
                Assert.That(taskResult.IsFaulted, Is.False);
                Assert.That(taskResult.Status, Is.EqualTo(TaskStatus.WaitingForActivation));

                Console.WriteLine("Starting server to establish connection...");
                using (var server = new FakeTcpServer(_log, 8999))
                {
                    server.OnClientConnected += () => Console.WriteLine("Client connected...");
                    server.OnBytesReceived += (b) =>
                    {
                        server.SendDataAsync(MessageHelper.CreateMetadataResponse(1, "Test"));
                    };

                    await taskResult;

                    Assert.That(taskResult.IsFaulted, Is.False);
                    Assert.That(taskResult.IsCanceled, Is.False);
                    Assert.That(taskResult.Status, Is.EqualTo(TaskStatus.RanToCompletion));
                }
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task SendAsyncShouldTimeoutMultipleMessagesAtATime()
        {
            using (var server = new FakeTcpServer(_log, 8999))
            using (var socket = new KafkaTcpSocket(_log, _kafkaEndpoint))
            using (var conn = new KafkaConnection(socket, TimeSpan.FromMilliseconds(100), log: _log))
            {
                server.HasClientConnected.Wait(TimeSpan.FromSeconds(3));
                Assert.That(server.ConnectionEventcount, Is.EqualTo(1));

                var tasks = new[]
                    {
                        conn.SendAsync(new MetadataRequest()),
                        conn.SendAsync(new MetadataRequest()),
                        conn.SendAsync(new MetadataRequest())
                    };

                Task.WhenAll(tasks);

                await TaskTest.WaitFor(() => tasks.All(t => t.IsFaulted));
                foreach (var task in tasks)
                {
                    Assert.That(task.IsFaulted, Is.True, "Task should have faulted.");
                    Assert.That(task.Exception.InnerException, Is.TypeOf<ResponseTimeoutException>(),
                        "Task fault should be of type ResponseTimeoutException.");
                }
            }
        }

        #endregion Send Tests...

        private static byte[] CreateCorrelationMessage(int id)
        {
            return new KafkaMessagePacker().Pack(id).Payload();
        }
    }
}