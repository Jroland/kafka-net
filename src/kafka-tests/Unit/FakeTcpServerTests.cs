using System;
using System.Net.Sockets;
using System.Threading;
using KafkaNet.Common;
using kafka_tests.Fakes;
using NUnit.Framework;
using kafka_tests.Helpers;

namespace kafka_tests.Unit
{
    [TestFixture]
    [Category("unit")]
    public class FakeTcpServerTests
    {
        private readonly Uri _fakeServerUrl;

        public FakeTcpServerTests()
        {
            _fakeServerUrl = new Uri("http://localhost:8999");
        }

        [Test]
        public void FakeShouldBeAbleToReconnect()
        {
            using (var server = new FakeTcpServer(8999))
            {
                byte[] received = null;
                server.OnBytesReceived += data => received = data;

                var t1 = new TcpClient();
                t1.Connect(_fakeServerUrl.Host, _fakeServerUrl.Port);
                TaskTest.WaitFor(() => server.ConnectionEventcount == 1);

                server.DropConnection();
                TaskTest.WaitFor(() => server.DisconnectionEventCount == 1);

                var t2 = new TcpClient();
                t2.Connect(_fakeServerUrl.Host, _fakeServerUrl.Port);
                TaskTest.WaitFor(() => server.ConnectionEventcount == 2);

                t2.GetStream().Write(99.ToBytes(), 0, 4);
                TaskTest.WaitFor(() => received != null);

                Assert.That(received.ToInt32(), Is.EqualTo(99));
            }
        }

        [Test]
        public void ShouldDisposeEvenWhenTryingToSendWithoutExceptionThrown()
        {
            using (var server = new FakeTcpServer(8999))
            {
                server.SendDataAsync("test");
                Thread.Sleep(500);
            }
        }

        [Test]
        public void ShouldDisposeWithoutExecptionThrown()
        {
            using (var server = new FakeTcpServer(8999))
            {
                Thread.Sleep(500);
            }
        }

        [Test]
        public void SendAsyncShouldWaitUntilClientIsConnected()
        {
            const int testData = 99;
            using (var server = new FakeTcpServer(8999))
            using (var client = new TcpClient())
            {
                server.SendDataAsync(testData.ToBytes());
                Thread.Sleep(1000);
                client.Connect(_fakeServerUrl.Host, _fakeServerUrl.Port);

                var buffer = new byte[4];
                client.GetStream().ReadAsync(buffer, 0, 4).Wait(TimeSpan.FromSeconds(5));
                
                Assert.That(buffer.ToInt32(), Is.EqualTo(testData));
            }
        }
    }
}
