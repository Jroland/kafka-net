using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using KafkaNet.Common;
using NUnit.Framework;
using kafka_tests.Helpers;

namespace kafka_tests.Unit
{
    [TestFixture]
    [Category("unit")]
    public class FakeTcpServerTests
    {
        private readonly FakeTcpServer _fakeTcpServer;
        private readonly Uri _fakeServerUrl;

        public FakeTcpServerTests()
        {
            _fakeTcpServer = new FakeTcpServer();
            _fakeTcpServer.Start(8999);
            _fakeServerUrl = new Uri("http://localhost:8999");
        }

        [TestFixtureTearDown]
        public void TearDown()
        {
            _fakeTcpServer.End();
        }

        [Test]
        public void FakeShouldBeAbleToReconnect()
        {
            byte[] received = null;
            _fakeTcpServer.OnBytesReceived += data => received = data;
            
            var t1 = new TcpClient();
            t1.Connect(_fakeServerUrl.Host, _fakeServerUrl.Port);
            TaskTest.WaitFor(() => _fakeTcpServer.ConnectedClients == 1, 100000000);

            _fakeTcpServer.DropConnection();
            TaskTest.WaitFor(() => _fakeTcpServer.ConnectedClients == 0, 100000000);

            var t2 = new TcpClient();
            t2.Connect(_fakeServerUrl.Host, _fakeServerUrl.Port);
            TaskTest.WaitFor(() => _fakeTcpServer.ConnectedClients == 1, 100000000);

            t2.GetStream().Write(99.ToBytes(), 0, 4);
            TaskTest.WaitFor(() => received != null, 100000000);

            Assert.That(received.ToInt32(), Is.EqualTo(99));
        }

    }
}
