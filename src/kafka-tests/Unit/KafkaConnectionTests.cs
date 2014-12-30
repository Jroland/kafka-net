using System;
using System.Linq;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Common;
using KafkaNet.Model;
using KafkaNet.Protocol;
using Moq;
using NUnit.Framework;
using Ninject.MockingKernel.Moq;
using kafka_tests.Helpers;

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
        [Test]
        public void ShouldStartReadPollingOnConstruction()
        {
            using (var socket = new KafkaTcpSocket(_log, _kafkaEndpoint))
            using (var conn = new KafkaConnection(socket, log: _log))
            {
                TaskTest.WaitFor(() => conn.ReadPolling);
                Assert.That(conn.ReadPolling, Is.True);
            }
        }

        [Test]
        public void ShouldReportServerUriOnConstruction()
        {
            var expectedUrl = _kafkaEndpoint;
            using (var socket = new KafkaTcpSocket(_log, expectedUrl))
            using (var conn = new KafkaConnection(socket, log: _log))
            {
                Assert.That(conn.Endpoint, Is.EqualTo(expectedUrl));
            }
        }
        #endregion

        #region Dispose Tests...
        [Test]
        public void ShouldDisposeWithoutExceptionThrown()
        {
            using (var server = new FakeTcpServer(8999))
            using (var socket = new KafkaTcpSocket(_log, _kafkaEndpoint))
            {
                var conn = new KafkaConnection(socket, log: _log);
                TaskTest.WaitFor(() => server.ConnectionEventcount > 0);
                using (conn) { }
            }
        }

        [Test]
        public void ShouldDisposeWithoutExceptionEvenWhileCallingSendAsync()
        {
            using (var socket = new KafkaTcpSocket(_log, _kafkaEndpoint))
            using (var conn = new KafkaConnection(socket, log: _log))
            {
                var task = conn.SendAsync(new MetadataRequest());
                task.Wait(TimeSpan.FromMilliseconds(1000));
                Assert.That(task.IsCompleted, Is.False, "The send task should still be pending.");
            }
        }
        #endregion

        #region Read Tests...
        [Test]
        public void ReadShouldLogDisconnectAndRecover()
        {
            var mockLog = _kernel.GetMock<IKafkaLog>();

            using (var server = new FakeTcpServer(8999))
            using (var socket = new KafkaTcpSocket(mockLog.Object, _kafkaEndpoint))
            using (var conn = new KafkaConnection(socket, log: mockLog.Object))
            {
                TaskTest.WaitFor(() => server.ConnectionEventcount > 0);
                Assert.That(server.ConnectionEventcount, Is.EqualTo(1));

                server.DropConnection();
                TaskTest.WaitFor(() => server.DisconnectionEventCount > 0);
                Assert.That(server.DisconnectionEventCount, Is.EqualTo(1));

                //should log an exception and keep going
                mockLog.Verify(x => x.ErrorFormat(It.IsAny<string>(), It.IsAny<Exception>()));

                TaskTest.WaitFor(() => server.ConnectionEventcount > 1);
                Assert.That(server.ConnectionEventcount, Is.EqualTo(2));
            }
        }

        [Test]
        public void ReadShouldIgnoreMessageWithUnknownCorrelationId()
        {
            const int correlationId = 99;

            var mockLog = _kernel.GetMock<IKafkaLog>();

            using (var server = new FakeTcpServer(8999))
            using (var socket = new KafkaTcpSocket(mockLog.Object, _kafkaEndpoint))
            using (var conn = new KafkaConnection(socket, log: mockLog.Object))
            {
                //send correlation message
                server.SendDataAsync(CreateCorrelationMessage(correlationId)).Wait(TimeSpan.FromSeconds(5));

                //wait for connection
                TaskTest.WaitFor(() => server.ConnectionEventcount > 0);
                Assert.That(server.ConnectionEventcount, Is.EqualTo(1));

                //should log a warning and keep going
                mockLog.Verify(x => x.WarnFormat(It.IsAny<string>(), It.Is<int>(o => o == correlationId)));
            }
        }
        #endregion

        #region Send Tests...
        [Test]
        public void SendAsyncShouldTimeoutByThrowingResponseTimeoutException()
        {
            using (var server = new FakeTcpServer(8999))
            using (var socket = new KafkaTcpSocket(_log, _kafkaEndpoint))
            using (var conn = new KafkaConnection(socket, TimeSpan.FromMilliseconds(100), log: _log))
            {
                TaskTest.WaitFor(() => server.ConnectionEventcount > 0);
                Assert.That(server.ConnectionEventcount, Is.EqualTo(1));

                var taskResult = conn.SendAsync(new MetadataRequest());

                taskResult.ContinueWith(t => taskResult = t).Wait(TimeSpan.FromSeconds(1));

                Assert.That(taskResult.IsFaulted, Is.True);
                Assert.That(taskResult.Exception.InnerException, Is.TypeOf<ResponseTimeoutException>());
            }
        }

        [Test]
        public void SendAsyncShouldTimeoutMultipleMessagesAtATime()
        {
            using (var server = new FakeTcpServer(8999))
            using (var socket = new KafkaTcpSocket(_log, _kafkaEndpoint))
            using (var conn = new KafkaConnection(socket, TimeSpan.FromMilliseconds(100), log: _log))
            {
                TaskTest.WaitFor(() => server.ConnectionEventcount > 0);
                Assert.That(server.ConnectionEventcount, Is.EqualTo(1));

                var tasks = new[]
                    {
                        conn.SendAsync(new MetadataRequest()),
                        conn.SendAsync(new MetadataRequest()),
                        conn.SendAsync(new MetadataRequest())
                    };

                Task.WhenAll(tasks);

                TaskTest.WaitFor(() => tasks.Any(t => t.IsFaulted));
                foreach (var task in tasks)
                {
                    Assert.That(task.IsFaulted, Is.True);
                    Assert.That(task.Exception.InnerException, Is.TypeOf<ResponseTimeoutException>());
                }
            }
        }
        #endregion

        private static byte[] CreateCorrelationMessage(int id)
        {
            return new KafkaMessagePacker().Pack(id).Payload();
        }
    }
}
