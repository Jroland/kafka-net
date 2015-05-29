using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Common;
using KafkaNet.Model;
using KafkaNet.Protocol;
using kafka_tests.Fakes;
using NUnit.Framework;
using kafka_tests.Helpers;
using System.Collections.Generic;

namespace kafka_tests.Unit
{
    /// <summary>
    /// Note these integration tests require an actively running kafka server defined in the app.config file.
    /// </summary>
    [TestFixture]
    [Category("unit")]
    public class KafkaTcpSocketTests
    {
        private const int FakeServerPort = 8999;
        private readonly KafkaEndpoint _fakeServerUrl;
        private readonly KafkaEndpoint _badServerUrl;

        public KafkaTcpSocketTests()
        {
            var log = new DefaultTraceLog();
            _fakeServerUrl = new DefaultKafkaConnectionFactory().Resolve(new Uri("http://localhost:8999"), log);
            _badServerUrl = new DefaultKafkaConnectionFactory().Resolve(new Uri("http://localhost:1"), log);
        }

        [Test]
        public void KafkaTcpSocketShouldConstruct()
        {
            using (var test = new KafkaTcpSocket(new DefaultTraceLog(), _fakeServerUrl))
            {

                Assert.That(test, Is.Not.Null);
                Assert.That(test.Endpoint, Is.EqualTo(_fakeServerUrl));
            }
        }

        #region Connection Tests...
        [Test]
        public void ConnectionShouldStartDedicatedThreadOnConstruction()
        {
            var count = 0;

            using (var test = new KafkaTcpSocket(new DefaultTraceLog(), _fakeServerUrl))
            {
                test.OnReconnectionAttempt += x => Interlocked.Increment(ref count);
                TaskTest.WaitFor(() => count > 0);
                Assert.That(count, Is.GreaterThan(0));
            }
        }

        [Test]
        public void ConnectionShouldAttemptMultipleTimesWhenConnectionFails()
        {
            var count = 0;
            using (var test = new KafkaTcpSocket(new DefaultTraceLog(), _badServerUrl))
            {
                test.WriteAsync(1.ToBytes().ToPayload()); //will force a connection
                test.OnReconnectionAttempt += x => Interlocked.Increment(ref count);
                TaskTest.WaitFor(() => count > 1, 10000);
                Assert.That(count, Is.GreaterThan(1));
            }
        }

        #endregion

        #region Dispose Tests...
        [Test]
        public void KafkaTcpSocketShouldDisposeEvenWhilePollingToReconnect()
        {
            int connectionAttempt = 0;
            using (var test = new KafkaTcpSocket(new DefaultTraceLog(), _fakeServerUrl))
            {
                test.OnReconnectionAttempt += i => connectionAttempt = i;

                var taskResult = test.ReadAsync(4);

                TaskTest.WaitFor(() => connectionAttempt > 1);

                using (test) { }

                taskResult.ContinueWith(t => taskResult = t).Wait(TimeSpan.FromSeconds(1));

                Assert.That(taskResult.IsFaulted, Is.True);
                Assert.That(taskResult.Exception.InnerException, Is.TypeOf<ObjectDisposedException>());
            }
        }

        [Test]
        public void KafkaTcpSocketShouldDisposeEvenWhileAwaitingReadAndThrowException()
        {
            using (var server = new FakeTcpServer(FakeServerPort))
            using (var test = new KafkaTcpSocket(new DefaultTraceLog(), _fakeServerUrl))
            {
                int readSize = 0;

                test.OnReadFromSocketAttempt += i => readSize = i;

                var taskResult = test.ReadAsync(4);

                TaskTest.WaitFor(() => readSize > 0);

                using (test) { }

                taskResult.ContinueWith(t => taskResult = t).Wait(TimeSpan.FromSeconds(1));

                Assert.That(taskResult.IsFaulted, Is.True);
                Assert.That(taskResult.Exception.InnerException, Is.TypeOf<ObjectDisposedException>());
            }
        }

        [Test]
        public void KafkaTcpSocketShouldDisposeEvenWhileWriting()
        {
            using (var test = new KafkaTcpSocket(new DefaultTraceLog(), _fakeServerUrl))
            {
                int writeSize = 0;
                test.OnWriteToSocketAttempt += payload => writeSize = payload.Buffer.Length;

                var taskResult = test.WriteAsync(4.ToBytes().ToPayload());

                TaskTest.WaitFor(() => writeSize > 0);

                using (test)
                {
                } //allow the sockets to set

                taskResult.ContinueWith(t => taskResult = t).Wait(TimeSpan.FromSeconds(20));

                Assert.That(taskResult.IsCompleted, Is.True);
                Assert.That(taskResult.IsFaulted, Is.True, "Task should result indicate a fault.");
                Assert.That(taskResult.Exception.InnerException, Is.TypeOf<ObjectDisposedException>(),
                    "Exception should be a disposed exception.");
            }
        }
        #endregion

        #region Read Tests...
        [Test]
        public void ReadShouldCancelWhileAwaitingResponse()
        {
            using (var server = new FakeTcpServer(FakeServerPort))
            using (var test = new KafkaTcpSocket(new DefaultTraceLog(), _fakeServerUrl))
            {
                var count = 0;
                var semaphore = new SemaphoreSlim(0);
                var token = new CancellationTokenSource();

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
        public void ReadShouldCancelWhileAwaitingReconnection()
        {
            int connectionAttempt = 0;
            using (var test = new KafkaTcpSocket(new DefaultTraceLog(), _fakeServerUrl))
            using (var token = new CancellationTokenSource())
            {
                test.OnReconnectionAttempt += i => connectionAttempt = i;

                var taskResult = test.ReadAsync(4, token.Token);

                TaskTest.WaitFor(() => connectionAttempt > 1);

                token.Cancel();

                taskResult.SafeWait(TimeSpan.FromMilliseconds(1000));

                Assert.That(taskResult.IsCanceled, Is.True);
            }
        }

        [Test]
        public void SocketShouldReconnectEvenAfterCancelledRead()
        {
            int connectionAttempt = 0;
            using (var test = new KafkaTcpSocket(new DefaultTraceLog(), _fakeServerUrl))
            using (var token = new CancellationTokenSource())
            {
                test.OnReconnectionAttempt += i => Interlocked.Exchange(ref connectionAttempt, i);

                var taskResult = test.ReadAsync(4, token.Token);

                TaskTest.WaitFor(() => connectionAttempt > 1);

                var attemptsMadeSoFar = connectionAttempt;

                token.Cancel();

                TaskTest.WaitFor(() => connectionAttempt > attemptsMadeSoFar);

                Assert.That(connectionAttempt, Is.GreaterThan(attemptsMadeSoFar));
            }
        }

        [Test]
        public void ReadShouldBlockUntilAllBytesRequestedAreReceived()
        {
            using (var server = new FakeTcpServer(FakeServerPort))
            using (var test = new KafkaTcpSocket(new DefaultTraceLog(), _fakeServerUrl))
            {
                var sendCompleted = 0;
                var bytesReceived = 0;

                test.OnBytesReceived += i => Interlocked.Add(ref bytesReceived, i);

                var resultTask = test.ReadAsync(4).ContinueWith(t =>
                    {
                        Interlocked.Increment(ref sendCompleted);
                        return t.Result;
                    });

                Console.WriteLine("Sending first 3 bytes...");
                server.HasClientConnected.Wait(TimeSpan.FromMilliseconds(1000));
                var sendInitialBytes = server.SendDataAsync(new byte[] { 0, 0, 0 }).Wait(TimeSpan.FromSeconds(10));
                Assert.That(sendInitialBytes, Is.True, "First 3 bytes should have been sent.");

                Console.WriteLine("Ensuring task blocks...");
                TaskTest.WaitFor(() => bytesReceived > 0);
                Assert.That(resultTask.IsCompleted, Is.False, "Task should still be running, blocking.");
                Assert.That(sendCompleted, Is.EqualTo(0), "Should still block even though bytes have been received.");
                Assert.That(bytesReceived, Is.EqualTo(3), "Three bytes should have been received and we are waiting on the last byte.");

                Console.WriteLine("Sending last byte...");
                var sendLastByte = server.SendDataAsync(new byte[] { 0 }).Wait(TimeSpan.FromSeconds(10));
                Assert.That(sendLastByte, Is.True, "Last byte should have sent.");

                Console.WriteLine("Ensuring task unblocks...");
                TaskTest.WaitFor(() => resultTask.IsCompleted);
                Assert.That(bytesReceived, Is.EqualTo(4), "Should have received 4 bytes.");
                Assert.That(resultTask.IsCompleted, Is.True, "Task should have completed.");
                Assert.That(sendCompleted, Is.EqualTo(1), "Task ContinueWith should have executed.");
                Assert.That(resultTask.Result.Length, Is.EqualTo(4), "Result of task should be 4 bytes.");
            }
        }

        [Test]
        public void ReadShouldBeAbleToReceiveMoreThanOnce()
        {
            using (var server = new FakeTcpServer(FakeServerPort))
            using (var test = new KafkaTcpSocket(new DefaultTraceLog(), _fakeServerUrl))
            {
                const int firstMessage = 99;
                const string secondMessage = "testmessage";

                Console.WriteLine("Sending first message to receive...");
                server.SendDataAsync(firstMessage.ToBytes());

                var firstResponse = test.ReadAsync(4).Result.ToInt32();
                Assert.That(firstResponse, Is.EqualTo(firstMessage));

                Console.WriteLine("Sending second message to receive...");
                server.SendDataAsync(secondMessage);

                var secondResponse = Encoding.ASCII.GetString(test.ReadAsync(secondMessage.Length).Result);
                Assert.That(secondResponse, Is.EqualTo(secondMessage));
            }
        }

        [Test]
        public void ReadShouldBeAbleToReceiveMoreThanOnceAsyncronously()
        {
            using (var server = new FakeTcpServer(FakeServerPort))
            using (var test = new KafkaTcpSocket(new DefaultTraceLog(), _fakeServerUrl))
            {
                const int firstMessage = 99;
                const int secondMessage = 100;

                Console.WriteLine("Sending first message to receive...");
                server.SendDataAsync(firstMessage.ToBytes());
                var firstResponseTask = test.ReadAsync(4);

                Console.WriteLine("Sending second message to receive...");
                server.SendDataAsync(secondMessage.ToBytes());
                var secondResponseTask = test.ReadAsync(4);

                Assert.That(firstResponseTask.Result.ToInt32(), Is.EqualTo(firstMessage));
                Assert.That(secondResponseTask.Result.ToInt32(), Is.EqualTo(secondMessage));
            }
        }

        [Test]
        public void ReadShouldNotLoseDataFromStreamOverMultipleReads()
        {
            using (var server = new FakeTcpServer(FakeServerPort))
            using (var test = new KafkaTcpSocket(new DefaultTraceLog(), _fakeServerUrl))
            {
                const int firstMessage = 99;
                const string secondMessage = "testmessage";

                var payload = new KafkaMessagePacker()
                    .Pack(firstMessage)
                    .Pack(secondMessage, StringPrefixEncoding.None);

                //send the combined payload
                server.SendDataAsync(payload.PayloadNoLength());

                var firstResponse = test.ReadAsync(4).Result.ToInt32();
                Assert.That(firstResponse, Is.EqualTo(firstMessage));

                var secondResponse = Encoding.ASCII.GetString(test.ReadAsync(secondMessage.Length).Result);
                Assert.That(secondResponse, Is.EqualTo(secondMessage));
            }
        }

        [Test]
        public void ReadShouldThrowServerDisconnectedExceptionWhenDisconnected()
        {
            using (var server = new FakeTcpServer(FakeServerPort))
            {
                var socket = new KafkaTcpSocket(new DefaultTraceLog(), _fakeServerUrl);

                var resultTask = socket.ReadAsync(4);

                //wait till connected
                TaskTest.WaitFor(() => server.ConnectionEventcount > 0);

                server.DropConnection();

                TaskTest.WaitFor(() => server.DisconnectionEventCount > 0);

                resultTask.ContinueWith(t => resultTask = t).Wait(TimeSpan.FromSeconds(1));

                Assert.That(resultTask.IsFaulted, Is.True);
                Assert.That(resultTask.Exception.InnerException, Is.TypeOf<ServerDisconnectedException>());
            }

        }

        [Test]
        public void ReadShouldReconnectAfterLosingConnection()
        {
            using (var server = new FakeTcpServer(FakeServerPort))
            {
                var disconnects = 0;
                var connects = 0;
                server.OnClientConnected += () => Interlocked.Increment(ref connects);
                server.OnClientDisconnected += () => Interlocked.Increment(ref disconnects);
                var socket = new KafkaTcpSocket(new DefaultTraceLog(), _fakeServerUrl);

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

        [Test]
        public void ReadShouldStackReadRequestsAndReturnOneAtATime()
        {
            using (var server = new FakeTcpServer(FakeServerPort))
            {
                var messages = new[] { "test1", "test2", "test3", "test4" };
                var expectedLength = "test1".Length;

                var payload = new KafkaMessagePacker().Pack(messages);

                var socket = new KafkaTcpSocket(new DefaultTraceLog(), _fakeServerUrl);

                var tasks = messages.Select(x => socket.ReadAsync(x.Length)).ToArray();

                server.SendDataAsync(payload.Payload());

                Task.WaitAll(tasks);

                foreach (var task in tasks)
                {
                    Assert.That(task.Result.Length, Is.EqualTo(expectedLength));
                }
            }
        }
        #endregion

        #region Write Tests...
        [Test]
        public void WriteAsyncShouldSendData()
        {
            using (var server = new FakeTcpServer(FakeServerPort))
            using (var test = new KafkaTcpSocket(new DefaultTraceLog(), _fakeServerUrl))
            {
                const int testData = 99;
                int result = 0;

                server.OnBytesReceived += data => result = data.ToInt32();

                test.WriteAsync(testData.ToBytes().ToPayload()).Wait(TimeSpan.FromSeconds(1));
                TaskTest.WaitFor(() => result > 0);
                Assert.That(result, Is.EqualTo(testData));
            }
        }

        [Test]
        public void WriteAsyncShouldAllowMoreThanOneWrite()
        {
            using (var server = new FakeTcpServer(FakeServerPort))
            using (var test = new KafkaTcpSocket(new DefaultTraceLog(), _fakeServerUrl))
            {
                const int testData = 99;
                var results = new List<byte>();

                server.OnBytesReceived += results.AddRange;

                Task.WaitAll(test.WriteAsync(testData.ToBytes().ToPayload()), test.WriteAsync(testData.ToBytes().ToPayload()));
                TaskTest.WaitFor(() => results.Count >= 8);
                Assert.That(results.Count, Is.EqualTo(8));
            }
        }

        [Test]
        public void WriteAndReadShouldBeAsynchronous()
        {
            var write = new List<int>();
            var read = new List<int>();
            var expected = new List<int> { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

            using (var server = new FakeTcpServer(FakeServerPort))
            using (var test = new KafkaTcpSocket(new DefaultTraceLog(), _fakeServerUrl))
            {
                server.OnBytesReceived += data => write.AddRange(data.Batch(4).Select(x => x.ToArray().ToInt32()));

                var tasks = Enumerable.Range(1, 10)
                    .SelectMany(i => new[]
                    {
                        test.WriteAsync(i.ToBytes().ToPayload()),
                        test.ReadAsync(4).ContinueWith(t => read.Add(t.Result.ToInt32())),
                        server.SendDataAsync(i.ToBytes())
                    }).ToArray();

                Task.WaitAll(tasks);
                Assert.That(write.OrderBy(x => x), Is.EqualTo(expected));
                Assert.That(read.OrderBy(x => x), Is.EqualTo(expected));
            }
        }

        [Test]
        public void WriteShouldHandleLargeVolumeSendAsynchronously()
        {
            var write = new List<int>();

            using (var server = new FakeTcpServer(FakeServerPort))
            using (var test = new KafkaTcpSocket(new DefaultTraceLog(), _fakeServerUrl))
            {
                server.OnBytesReceived += data => write.AddRange(data.Batch(4).Select(x => x.ToArray().ToInt32()));

                var tasks = Enumerable.Range(1, 10000)
                    .SelectMany(i => new[]
                    {
                        test.WriteAsync(i.ToBytes().ToPayload()),
                    }).ToArray();

                Task.WaitAll(tasks);

                Assert.That(write.OrderBy(x => x), Is.EqualTo(Enumerable.Range(1, 10000)));
            }
        }

        [Test]
        public void WriteShouldCancelWhileSendingData()
        {
            var writeAttempts = 0;
            using (var server = new FakeTcpServer(FakeServerPort))
            using (var test = new KafkaTcpSocket(new DefaultTraceLog(), _fakeServerUrl))
            using (var token = new CancellationTokenSource())
            {
                test.OnWriteToSocketAttempt += payload => Interlocked.Increment(ref writeAttempts);

                test.WriteAsync(new KafkaDataPayload { Buffer = 1.ToBytes() }, token.Token);

                TaskTest.WaitFor(() => server.ConnectionEventcount > 0);
                TaskTest.WaitFor(() => writeAttempts > 0);

                Assert.That(writeAttempts, Is.EqualTo(1), "Socket should have attempted to write.");

                //create a buffer write that will take a long time
                var taskResult = test.WriteAsync(
                    new KafkaDataPayload { Buffer = Enumerable.Range(0, 1000000).Select(b => (byte)b).ToArray() },
                    token.Token);

                token.Cancel();

                taskResult.SafeWait(TimeSpan.FromMilliseconds(5000));

                Assert.That(taskResult.IsCanceled, Is.True, "Task should have cancelled.");
            }
        }

        [Test]
        public void WriteShouldCancelWhileAwaitingReconnection()
        {
            int connectionAttempt = 0;
            using (var test = new KafkaTcpSocket(new DefaultTraceLog(), _fakeServerUrl))
            using (var token = new CancellationTokenSource())
            {
                test.OnReconnectionAttempt += i => connectionAttempt = i;

                var taskResult = test.WriteAsync(new KafkaDataPayload { Buffer = 1.ToBytes() }, token.Token);

                TaskTest.WaitFor(() => connectionAttempt > 1);

                token.Cancel();

                taskResult.SafeWait(TimeSpan.FromMilliseconds(1000));

                Assert.That(taskResult.IsCanceled, Is.True);
            }
        }

        [Test]
        public void SocketShouldReconnectEvenAfterCancelledWrite()
        {
            int connectionAttempt = 0;
            using (var test = new KafkaTcpSocket(new DefaultTraceLog(), _fakeServerUrl))
            using (var token = new CancellationTokenSource())
            {
                test.OnReconnectionAttempt += i => Interlocked.Exchange(ref connectionAttempt, i);

                var taskResult = test.WriteAsync(new KafkaDataPayload { Buffer = 1.ToBytes() }, token.Token);

                TaskTest.WaitFor(() => connectionAttempt > 1);

                var attemptsMadeSoFar = connectionAttempt;

                token.Cancel();

                TaskTest.WaitFor(() => connectionAttempt > attemptsMadeSoFar);

                Assert.That(connectionAttempt, Is.GreaterThan(attemptsMadeSoFar));
            }
        }
        #endregion
    }
}
