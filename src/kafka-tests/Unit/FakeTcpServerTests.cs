﻿using kafka_tests.Fakes;
using kafka_tests.Helpers;
using KafkaNet;
using KafkaNet.Common;
using NUnit.Framework;
using System;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace kafka_tests.Unit
{
    [TestFixture]
    [Category("unit")]
    public class FakeTcpServerTests
    {
        private readonly Uri _fakeServerUrl;
        private IKafkaLog Ilog = new DefaultTraceLog(LogLevel.Warn);

        public FakeTcpServerTests()
        {
            _fakeServerUrl = new Uri("http://localhost:8999");
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task FakeShouldBeAbleToReconnect()
        {
            using (var server = new FakeTcpServer(Ilog, 8999))
            {
                byte[] received = null;
                server.OnBytesReceived += data => received = data;

                var t1 = new TcpClient();
                t1.Connect(_fakeServerUrl.Host, _fakeServerUrl.Port);
                await TaskTest.WaitFor(() => server.ConnectionEventcount == 1);

                server.DropConnection();
                await TaskTest.WaitFor(() => server.DisconnectionEventCount == 1);

                var t2 = new TcpClient();
                t2.Connect(_fakeServerUrl.Host, _fakeServerUrl.Port);
                await TaskTest.WaitFor(() => server.ConnectionEventcount == 2);

                t2.GetStream().Write(99.ToBytes(), 0, 4);
                await TaskTest.WaitFor(() => received != null);

                Assert.That(received.ToInt32(), Is.EqualTo(99));
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void ShouldDisposeEvenWhenTryingToSendWithoutExceptionThrown()
        {
            using (var server = new FakeTcpServer(Ilog, 8999))
            {
                server.SendDataAsync("test");
                Thread.Sleep(500);
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void ShouldDisposeWithoutExecptionThrown()
        {
            using (var server = new FakeTcpServer(Ilog, 8999))
            {
                Thread.Sleep(500);
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void SendAsyncShouldWaitUntilClientIsConnected()
        {
            const int testData = 99;
            using (var server = new FakeTcpServer(Ilog, 8999))
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