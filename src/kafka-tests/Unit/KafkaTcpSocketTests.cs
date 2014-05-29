using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Common;
using KafkaNet.Protocol;
using NUnit.Framework;
using kafka_tests.Fakes;
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
        private readonly Uri _fakeServerUrl;
        private readonly Uri _badServerUrl;

        public KafkaTcpSocketTests()
        {
            _fakeServerUrl = new Uri("http://localhost:8999");
            _badServerUrl = new Uri("http://localhost:1");
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

        #region Connection Tests...
        [Test]
        public void ConnectionShouldAttemptOnceOnConstruction()
        {
            var count = 0;

            using (var test = new KafkaTcpSocket(_fakeServerUrl, 20))
            {
                test.OnReconnectionAttempt += x => Interlocked.Increment(ref count);
                TaskTest.WaitFor(() => count > 0);
                Assert.That(count, Is.EqualTo(1));
            }
        }

        [Test]
        public void ConnectionShouldAttemptMultipleTimesWhenConnectionFails()
        {
            var count = 0;
            using (var test = new KafkaTcpSocket(_badServerUrl, 20))
            {
                test.OnReconnectionAttempt += x => Interlocked.Increment(ref count);
                TaskTest.WaitFor(() => count > 1, 3000);
                Assert.That(count, Is.GreaterThan(1));
            }
        }

        #endregion

        #region Dispose Tests...
        [Test]
        public void KafkaTcpSocketShouldDisposeEvenWhilePollingToReconnect()
        {
            var test = new KafkaTcpSocket(_fakeServerUrl);

            var taskResult = test.ReadAsync(4);

            using (test) { }

            taskResult.ContinueWith(t => taskResult = t).Wait(TimeSpan.FromSeconds(1));

            Assert.That(taskResult.IsFaulted, Is.True);
            Assert.That(taskResult.Exception.InnerException, Is.TypeOf<ObjectDisposedException>());
        }

        [Test]
        public void KafkaTcpSocketShouldDisposeEvenWhileAwaitingReadAndThrowException()
        {
            using (var server = new FakeTcpServer(8999))
            {
                var test = new KafkaTcpSocket(_fakeServerUrl);

                var taskResult = test.ReadAsync(4);

                using (test) { }

                taskResult.ContinueWith(t => taskResult = t).Wait(TimeSpan.FromSeconds(1));

                Assert.That(taskResult.IsFaulted, Is.True);
                Assert.That(taskResult.Exception.InnerException, Is.TypeOf<ObjectDisposedException>());
            }
        }

        [Test]
        public void KafkaTcpSocketShouldDisposeEvenWhileWriting()
        {
            var test = new KafkaTcpSocket(_fakeServerUrl);

            var taskResult = test.WriteAsync(4.ToBytes(), 0, 4);

            using (test) { }

            taskResult.ContinueWith(t => taskResult = t).Wait(TimeSpan.FromSeconds(1));

            Assert.That(taskResult.IsFaulted, Is.True);
            Assert.That(taskResult.Exception.InnerException, Is.TypeOf<ObjectDisposedException>());
        }
        #endregion

        #region Read Tests...
        [Test]
        public void ReadShouldCancelWhileAwaitingResponse()
        {
            using (var server = new FakeTcpServer(8999))
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
        }

        [Test]
        public void ReadShouldBlockUntilBytesRequestedAreReceived()
        {
            using (var server = new FakeTcpServer(8999))
            {
                var count = 0;

                var test = new KafkaTcpSocket(_fakeServerUrl);

                var resultTask = test.ReadAsync(4).ContinueWith(t =>
                    {
                        Interlocked.Increment(ref count);
                        return t.Result;
                    });

                //write 3 bytes
                server.SendDataAsync(new byte[] { 0, 0, 0 });
                Thread.Sleep(TimeSpan.FromMilliseconds(100));

                Assert.That(count, Is.EqualTo(0), "Should still block even though bytes have been received.");

                server.SendDataAsync(new byte[] { 0 });
                TaskTest.WaitFor(() => count > 0);
                Assert.That(count, Is.EqualTo(1), "Should unblock once all bytes are received.");
                Assert.That(resultTask.Result.Length, Is.EqualTo(4));
            }
        }

        [Test]
        public void ReadShouldBeAbleToReceiveMoreThanOnce()
        {
            using (var server = new FakeTcpServer(8999))
            {
                var count = 0;
                const string testMessage = "testmessage";
                const int testInteger = 99;

                int firstResponse = 0;
                string secondResponse = null;
                var test = new KafkaTcpSocket(_fakeServerUrl);

                test.ReadAsync(4).ContinueWith(t =>
                        {
                            firstResponse = t.Result.ToInt32();
                            Interlocked.Increment(ref count);
                        });

                server.SendDataAsync(testInteger.ToBytes());
                TaskTest.WaitFor(() => count > 0);
                Assert.That(count, Is.EqualTo(1));
                Assert.That(firstResponse, Is.EqualTo(testInteger));

                test.ReadAsync(testMessage.Length)
                    .ContinueWith(t =>
                    {
                        Interlocked.Increment(ref count);
                        secondResponse = Encoding.ASCII.GetString(t.Result);
                    });

                server.SendDataAsync(testMessage);
                TaskTest.WaitFor(() => count > 1);
                Assert.That(count, Is.EqualTo(2));
                Assert.That(secondResponse, Is.EqualTo(testMessage));
            }
        }

        [Test]
        public void ReadShouldNotLoseDataFromLargeStreamOverMultipleReads()
        {
            using (var server = new FakeTcpServer(8999))
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
                server.SendDataAsync(payload.Payload());

                TaskTest.WaitFor(() => count > 0);
                Assert.That(count, Is.EqualTo(1));
                Assert.That(firstResponse, Is.EqualTo(testInteger));

                test.ReadAsync(testMessage.Length)
                    .ContinueWith(t =>
                    {
                        Interlocked.Increment(ref count);
                        secondResponse = Encoding.ASCII.GetString(t.Result);
                    });

                server.SendDataAsync(testMessage);
                TaskTest.WaitFor(() => count > 1);
                Assert.That(count, Is.EqualTo(2));
                Assert.That(secondResponse, Is.EqualTo(testMessage));
            }
        }

        [Test]
        public void ReadShouldThrowServerDisconnectedExceptionWhenDisconnected()
        {
            using (var server = new FakeTcpServer(8999))
            {
                var connects = 0;
                server.OnClientConnected += () => Interlocked.Increment(ref connects);
                var socket = new KafkaTcpSocket(_fakeServerUrl);

                var resultTask = socket.ReadAsync(4);

                //wait till connected
                TaskTest.WaitFor(() => connects > 0);

                //drop connection
                server.DropConnection();

                resultTask.ContinueWith(t => resultTask = t).Wait(TimeSpan.FromSeconds(1));

                Assert.That(resultTask.IsFaulted, Is.True);
                Assert.That(resultTask.Exception.InnerException, Is.TypeOf<ServerDisconnectedException>());
            }

        }

        [Test]
        public void ReadShouldReconnectAfterLosingConnection()
        {
            using (var server = new FakeTcpServer(8999))
            {
                var disconnects = 0;
                var connects = 0;
                server.OnClientConnected += () => Interlocked.Increment(ref connects);
                server.OnClientDisconnected += () => Interlocked.Increment(ref disconnects);
                var socket = new KafkaTcpSocket(_fakeServerUrl);

                var resultTask = ReadFromSocketWithRetry(socket, 4);

                //wait till connected
                TaskTest.WaitFor(() => connects > 0);

                //drop connection
                server.DropConnection();
                TaskTest.WaitFor(() => disconnects > 0);
                Assert.That(disconnects, Is.EqualTo(1), "Server should have disconnected the client.");

                //wait for reconnection
                TaskTest.WaitFor(() => connects > 1);
                Assert.That(connects, Is.EqualTo(2), "Socket should have reconnected.");

                //send data and get result
                server.SendDataAsync(99.ToBytes());
                Assert.That(resultTask.Result.ToInt32(), Is.EqualTo(99), "Socket should have received the 4 bytes.");
            }

        }

        private async Task<byte[]> ReadFromSocketWithRetry(KafkaTcpSocket socket, int readSize)
        {
            byte[] buffer;
            try
            {
                buffer = await socket.ReadAsync(readSize);
                return buffer;
            }
            catch (Exception ex)
            {
                Assert.That(ex, Is.TypeOf<ServerDisconnectedException>());
            }

            buffer = await socket.ReadAsync(4);
            return buffer;
        }
        #endregion

        [Test]
        public void WriteAsyncShouldSendData()
        {
            using (var server = new FakeTcpServer(8999))
            {
                const int testData = 99;
                int result = 0;

                var test = new KafkaTcpSocket(_fakeServerUrl);
                server.OnBytesReceived += data => result = data.ToInt32();

                test.WriteAsync(testData.ToBytes(), 0, 4).Wait(TimeSpan.FromSeconds(1));
                TaskTest.WaitFor(() => result > 0);
                Assert.That(result, Is.EqualTo(testData));
            }
        }
    }
}
