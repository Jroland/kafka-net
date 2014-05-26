using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Model;
using NUnit.Framework;
using kafka_tests.Helpers;

namespace kafka_tests.Integration
{
    /// <summary>
    /// Note these integration tests require an actively running kafka server defined in the app.config file.
    /// </summary>
    [TestFixture]
    [Category("Integration")]
    public class KafkaTcpSocketTests
    {
        private readonly FakeTcpServer _fakeTcpServer;
        private readonly Uri _echoUrl;

        public KafkaTcpSocketTests()
        {
            _fakeTcpServer = new FakeTcpServer();
            _fakeTcpServer.Start(8999);
            _echoUrl = new Uri("http://localhost:8999");
        }

        [TestFixtureTearDown]
        public void TearDown()
        {
            _fakeTcpServer.End();
        }

        [Test]
        public void KafkaTcpSocketShouldConstruct()
        {
            using (var test = new KafkaTcpSocket(_echoUrl))
            {
                Assert.That(test, Is.Not.Null);
                Assert.That(test.ClientUri, Is.EqualTo(_echoUrl));
            }
        }

        [Test]
        public void KafkaTcpSocketShouldDisposeEvenWhileAwaitingRead()
        {
            var semaphore = new SemaphoreSlim(0);

            var test = new KafkaTcpSocket(_echoUrl);

            test.ReadAsync(4).ContinueWith(t =>
                {
                    Assert.That(t.IsFaulted, Is.True, "Task should have a disposed exception.");
                    Assert.That(t.Exception, Is.Not.Null, "Task should have a disposed exception.");
                    semaphore.Release();
                });

            using (test) { }

            semaphore.Wait(TimeSpan.FromSeconds(1));
        }

        [Test]
        [Ignore("The cancel token does not seem to be working with the current version of TcpClient.ReadAsync")]
        public void KafkaTcpSocketShouldCancelWhileAwaitingRead()
        {
            var count = 0;
            var semaphore = new SemaphoreSlim(0);
            var token = new CancellationTokenSource();

            var test = new KafkaTcpSocket(_echoUrl);

            test.ReadAsync(4, token.Token).ContinueWith(t =>
                {
                    Interlocked.Increment(ref count);
                    Assert.That(t.IsCanceled, Is.True, "Task should be set to cancelled when disposed.");
                    semaphore.Release();
                });

            token.Cancel();

            semaphore.Wait(TimeSpan.FromSeconds(1));
            Assert.That(count, Is.EqualTo(1), "Read should have cancelled and incremented count.");
        }

        [Test]
        public void KafkaTcpSocketShouldBlockUntilBytesRequestedAreReceived()
        {
            var count = 0;

            var test = new KafkaTcpSocket(_echoUrl);

            test.ReadAsync(4).ContinueWith(t =>
                {
                    Interlocked.Increment(ref count);
                });

            //write 3 bytes
            _fakeTcpServer.SendDataAsync(new byte[] {0, 0, 0});
            Thread.Sleep(TimeSpan.FromMilliseconds(100));

            Assert.That(count, Is.EqualTo(0), "Should still block even though bytes have been received.");

            _fakeTcpServer.SendDataAsync(new byte[] {0});
            TaskTest.WaitFor(() => count > 0);
            Assert.That(count, Is.EqualTo(1), "Should unblock once all bytes are received.");
        }
    }
}
