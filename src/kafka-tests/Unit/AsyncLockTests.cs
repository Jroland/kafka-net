using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet.Common;
using kafka_tests.Helpers;
using NUnit.Framework;

namespace kafka_tests.Unit
{
    [TestFixture]
    [Category("Unit")]
    public class AsyncLockTests
    {
        [Test]
        [ExpectedException(typeof(OperationCanceledException))]
        public async void AsyncLockCancelShouldThrowOperationCanceledException()
        {
            var count = 0;
            var token = new CancellationTokenSource(TimeSpan.FromMilliseconds(10));
            var alock = new AsyncLock();

            for (int i = 0; i < 2; i++)
            {
                //the second call will timeout
                using (await alock.LockAsync(token.Token))
                {
                    Interlocked.Increment(ref count);
                    Thread.Sleep(100);
                }
            }
            Assert.That(count, Is.EqualTo(1), "Only the first call should succeed.  The second should timeout.");
        }

        [Test]
        public async void AsyncLockCancelShouldNotAllowInsideLock()
        {
            var count = 0;
            var token = new CancellationTokenSource(TimeSpan.FromMilliseconds(10));
            var alock = new AsyncLock();

            try
            {
                for (int i = 0; i < 2; i++)
                {
                    //the second call will timeout
                    using (await alock.LockAsync(token.Token))
                    {
                        Interlocked.Increment(ref count);
                        Thread.Sleep(100);
                    }
                }
            }
            catch 
            {
            }

            Assert.That(count, Is.EqualTo(1));
        }

        [Test]
        public void AsyncLockShouldAllowMultipleStackedWaits()
        {
            var count = 0;
            var alock = new AsyncLock();
            var locks = new List<Task<AsyncLock.Releaser>>();
            for (int i = 0; i < 1000; i++)
            {
                var task = alock.LockAsync();
                task.ContinueWith(t => Interlocked.Increment(ref count));
                locks.Add(task);
            }

            for (int i = 0; i < 100; i++)
            {
                using (locks[i].Result)
                {
                    Thread.Sleep(10);
                    Assert.That(count, Is.EqualTo(i + 1));
                }
            }
        }

        [Test]
        public void AsyncLockShouldAllowOnlyOneThread()
        {
            var block = new SemaphoreSlim(0, 2);
            var count = 0;
            var alock = new AsyncLock();

            var firstCall = Task.Run(async () =>
            {
                using (await alock.LockAsync())
                {
                    Interlocked.Increment(ref count);
                    block.Wait();
                }
                block.Wait();//keep this thread busy
            });

            TaskTest.WaitFor(() => count > 0);

            alock.LockAsync().ContinueWith(t => Interlocked.Increment(ref count));

            Assert.That(count, Is.EqualTo(1), "Only one task should have gotten past lock.");
            Assert.That(firstCall.IsCompleted, Is.False, "Task should still be running.");

            block.Release();
            TaskTest.WaitFor(() => count > 1);
            Assert.That(count, Is.EqualTo(2), "Second call should get past lock.");
            Assert.That(firstCall.IsCompleted, Is.False, "First call should still be busy.");
            block.Release();
        }


        [Test]
        public void AsyncLockShouldUnlockEvenFromDifferentThreads()
        {
            var block = new SemaphoreSlim(0, 2);
            var count = 0;
            var alock = new AsyncLock();

            Task.Factory.StartNew(async () =>
            {
                using (await alock.LockAsync().ConfigureAwait(false))
                {
                    Console.WriteLine("Enter lock id: {0}", Thread.CurrentThread.ManagedThreadId);
                    Interlocked.Increment(ref count);
                    await ExternalThread();
                    await block.WaitAsync();
                    Console.WriteLine("Exit lock id: {0}", Thread.CurrentThread.ManagedThreadId);
                }
            });

            TaskTest.WaitFor(() => count > 0);

            Task.Factory.StartNew(async () =>
            {
                Console.WriteLine("Second call waiting Id:{0}", Thread.CurrentThread.ManagedThreadId);
                using (await alock.LockAsync().ConfigureAwait(false))
                {
                    Console.WriteLine("Past lock Id:{0}", Thread.CurrentThread.ManagedThreadId);
                    Interlocked.Increment(ref count);
                }
            });

            Assert.That(count, Is.EqualTo(1), "Only one task should have gotten past lock.");

            block.Release();
            TaskTest.WaitFor(() => count > 1);
            Assert.That(count, Is.EqualTo(2), "Second call should get past lock.");
        }

        private async Task ExternalThread()
        {
            var client = new HttpClient();
            await client.GetAsync("http://www.google.com");
            Thread.Sleep(1000);
        }
    }
}
