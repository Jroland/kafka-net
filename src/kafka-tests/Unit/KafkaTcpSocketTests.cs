using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Common;
using KafkaNet.Model;
using NUnit.Framework;
using kafka_tests.Helpers;

namespace kafka_tests.Integration
{
    /// <summary>
    /// Note these integration tests require an actively running kafka server defined in the app.config file.
    /// </summary>
    [TestFixture]
    [Category("unit")]
    public class KafkaTcpSocketTests
    {
        private readonly FakeTcpServer _fakeTcpServer;
        private readonly Uri _fakeServerUrl;

        public KafkaTcpSocketTests()
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
        public void KafkaTcpSocketShouldConstruct()
        {
            using (var test = new KafkaTcpSocket(_fakeServerUrl))
            {
                Assert.That(test, Is.Not.Null);
                Assert.That(test.ClientUri, Is.EqualTo(_fakeServerUrl));
            }
        }

        #region Dispose Tests...
        [Test]
        public void KafkaTcpSocketShouldDisposeEvenWhileAwaitingRead()
        {
            var semaphore = new SemaphoreSlim(0);

            var test = new KafkaTcpSocket(_fakeServerUrl);

            test.ReadAsync(4).ContinueWith(t =>
                {
                    Assert.That(t.IsFaulted, Is.True, "Task should have a disposed exception.");
                    Assert.That(t.Exception, Is.Not.Null, "Task should have a disposed exception.");
                    semaphore.Release();
                });

            using (test) { }

            semaphore.Wait(TimeSpan.FromSeconds(1));
        } 
        #endregion

        #region Read Tests...
        [Test]
        public void KafkaTcpSocketShouldCancelWhileAwaitingRead()
        {
            var count = 0;
            var semaphore = new SemaphoreSlim(0);
            var token = new CancellationTokenSource();

            var test = new KafkaTcpSocket(_fakeServerUrl);

            test.ReadAsync(4, token.Token).ContinueWith(t =>
                {
                    Interlocked.Increment(ref count);
                    Assert.That(t.IsCanceled, Is.True, "Task should be set to cancelled when disposed.");
                    semaphore.Release();
                });

            Thread.Sleep(100);
            token.Cancel();

            semaphore.Wait(TimeSpan.FromSeconds(1));
            Assert.That(count, Is.EqualTo(1), "Read should have cancelled and incremented count.");
        }

        [Test]
        public void KafkaTcpSocketShouldBlockUntilBytesRequestedAreReceived()
        {
            var count = 0;

            var test = new KafkaTcpSocket(_fakeServerUrl);

            var resultTask = test.ReadAsync(4).ContinueWith(t =>
                {
                    Interlocked.Increment(ref count);
                    return t.Result;
                });

            //write 3 bytes
            _fakeTcpServer.SendDataAsync(new byte[] { 0, 0, 0 });
            Thread.Sleep(TimeSpan.FromMilliseconds(100));

            Assert.That(count, Is.EqualTo(0), "Should still block even though bytes have been received.");

            _fakeTcpServer.SendDataAsync(new byte[] { 0 });
            TaskTest.WaitFor(() => count > 0);
            Assert.That(count, Is.EqualTo(1), "Should unblock once all bytes are received.");
            Assert.That(resultTask.Result.Length, Is.EqualTo(4));
        }

        [Test]
        public void KafkaTcpSocketShouldBeAbleToReceiveMoreThanOnce()
        {
            var count = 0;
            const string testMessage = "testmessage";
            const int testInteger = 99;

            int firstResponse = 0;
            string secondResponse = null;
            var test = new KafkaTcpSocket(_fakeServerUrl);

            test.ReadAsync(4).ContinueWith(t =>
                    {
                        Interlocked.Increment(ref count);
                        firstResponse = t.Result.ToInt32();
                    });

            _fakeTcpServer.SendDataAsync(testInteger.ToBytes());
            TaskTest.WaitFor(() => count > 0);
            Assert.That(count, Is.EqualTo(1));
            Assert.That(firstResponse, Is.EqualTo(testInteger));

            test.ReadAsync(testMessage.Length)
                .ContinueWith(t =>
                {
                    Interlocked.Increment(ref count);
                    secondResponse = Encoding.ASCII.GetString(t.Result);
                });

            _fakeTcpServer.SendDataAsync(testMessage);
            TaskTest.WaitFor(() => count > 1);
            Assert.That(count, Is.EqualTo(2));
            Assert.That(secondResponse, Is.EqualTo(testMessage));
        }

        [Test]
        public void KafkaTcpSocketShouldNotLoseDataFromLargeStreamOverMultipleReads()
        {
            var count = 0;
            const string testMessage = "testmessage";
            const int testInteger = 99;

            var payload = new WriteByteStream();
            payload.Pack(testInteger.ToBytes(), testMessage.ToBytes());

            int firstResponse = 0;
            string secondResponse = null;
            var test = new KafkaTcpSocket(_fakeServerUrl);

            test.ReadAsync(4).ContinueWith(t =>
            {
                Interlocked.Increment(ref count);
                firstResponse = t.Result.ToInt32();
            });

            //send the combined payload
            _fakeTcpServer.SendDataAsync(payload.Payload());

            TaskTest.WaitFor(() => count > 0);
            Assert.That(count, Is.EqualTo(1));
            Assert.That(firstResponse, Is.EqualTo(testInteger));

            test.ReadAsync(testMessage.Length)
                .ContinueWith(t =>
                {
                    Interlocked.Increment(ref count);
                    secondResponse = Encoding.ASCII.GetString(t.Result);
                });

            _fakeTcpServer.SendDataAsync(testMessage);
            TaskTest.WaitFor(() => count > 1);
            Assert.That(count, Is.EqualTo(2));
            Assert.That(secondResponse, Is.EqualTo(testMessage));
        } 
        #endregion

        [Test]
        public void WriteAsyncShouldSendData()
        {
            const int testData = 99;
            int result = 0;

            var test = new KafkaTcpSocket(_fakeServerUrl);
            _fakeTcpServer.OnBytesReceived += data => result = data.ToInt32();

            test.WriteAsync(testData.ToBytes(), 0, 4).Wait(TimeSpan.FromSeconds(1));
            TaskTest.WaitFor(() => result > 0);
            Assert.That(result, Is.EqualTo(testData));
        }
    }
}
