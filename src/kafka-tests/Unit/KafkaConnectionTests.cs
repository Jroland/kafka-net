using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Common;
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
        private MoqMockingKernel _kernel;

        public KafkaConnectionTests()
        {
            _log = new DefaultTraceLog();
        }

        [SetUp]
        public void Setup()
        {
            _kernel = new MoqMockingKernel();
        }

        #region Dispose Tests...
        [Test]
        public void ShouldDisposeWithoutExceptionThrown()
        {
            using (var server = new FakeTcpServer(8999))
            using (var socket = new KafkaTcpSocket(_log, new Uri("http://localhost:8999")))
            {
                var conn = new KafkaConnection(socket, log: _log);
                TaskTest.WaitFor(() => server.ConnectionEventcount > 0);
                using (conn) { }
            }
        }
        #endregion

        #region Read Tests...
        [Test]
        public void ReadShouldLogDisconnectAndRecover()
        {
            var mockLog = _kernel.GetMock<IKafkaLog>();

            using (var server = new FakeTcpServer(8999))
            using (var socket = new KafkaTcpSocket(mockLog.Object, new Uri("http://localhost:8999")))
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
            using (var socket = new KafkaTcpSocket(mockLog.Object, new Uri("http://localhost:8999")))
            using (var conn = new KafkaConnection(socket, log: mockLog.Object))
            {
                //send correlation message
                server.SendDataAsync(CreateCorrelationMessage(correlationId)).Wait(TimeSpan.FromSeconds(1));

                //wait for connection
                TaskTest.WaitFor(() => server.ConnectionEventcount > 0);
                Assert.That(server.ConnectionEventcount, Is.EqualTo(1));

                //should log an exception and keep going
                mockLog.Verify(x => x.WarnFormat(It.IsAny<string>(), correlationId));
            }
        }
        #endregion

        #region Send Tests...
        [Test]
        public void SendAsyncShouldTimeoutByThrowingResponseTimeoutException()
        {
            using (var server = new FakeTcpServer(8999))
            using (var socket = new KafkaTcpSocket(_log, new Uri("http://localhost:8999")))
            using (var conn = new KafkaConnection(socket, 100, log: _log))
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
            using (var socket = new KafkaTcpSocket(_log, new Uri("http://localhost:8999")))
            using (var conn = new KafkaConnection(socket, 100, log: _log))
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

                TaskTest.WaitFor(() => tasks.Any(t => t.IsFaulted ));
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
            var stream = new WriteByteStream();
            stream.Pack(4.ToBytes(), id.ToBytes());
            return stream.Payload();
        }
    }
}
