using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet.Common;
using kafka_tests.Helpers;
using NUnit.Framework;

namespace kafka_tests.Unit
{
    [TestFixture]
    public class NagleBlockingCollectionTests
    {
        [Test]
        public void NagleBlockingCollectionConstructs()
        {
            var collection = new NagleBlockingCollection<string>(10);
            Assert.That(collection, Is.Not.Null);
        }

        [Test]
        public async void EnsureCollectionBlocksAtCapacity()
        {
            const int blockingCount = 10;
            const int expectedCount = 5;

            var collection = new NagleBlockingCollection<int>(blockingCount);

            var addTask = Task.Factory.StartNew(() => collection.AddRangeAsync(Enumerable.Range(0, blockingCount + expectedCount), 
                CancellationToken.None).Wait());

            TaskTest.WaitFor(() => collection.Count >= blockingCount);

            Assert.That(collection.Count, Is.EqualTo(blockingCount), "The collection should only contain 10 items.");
            Assert.That(addTask.Status, Is.EqualTo(TaskStatus.Running), "The task should be blocking.");

            //unblock the collection
            await collection.TakeBatch(blockingCount, TimeSpan.FromMilliseconds(100), CancellationToken.None);

            await addTask;

            Assert.That(collection.Count, Is.EqualTo(expectedCount));
            Assert.That(addTask.Status, Is.EqualTo(TaskStatus.RanToCompletion));
        }

        [Test]
        public void CollectionAddAsyncShouldBeAbleToCancel()
        {
            var collection = new NagleBlockingCollection<int>(1);
            using (var cancellationToken = new CancellationTokenSource())
            {
                collection.AddAsync(1, CancellationToken.None);
                var cancelledTask = collection.AddAsync(1, cancellationToken.Token);

                cancellationToken.Cancel();
                Assert.That(cancelledTask.IsCanceled, Is.True);
            }

        }

        [Test]
        [ExpectedException(typeof(OperationCanceledException))]
        public async void CollectionAddRangeAsyncShouldBeAbleToCancel()
        {
            var collection = new NagleBlockingCollection<int>(1);
            using (var cancellationToken = new CancellationTokenSource())
            {
                var cancelledTask = collection.AddRangeAsync(Enumerable.Range(0, 5), cancellationToken.Token);
                cancellationToken.Cancel();
                await cancelledTask;
            }

        }


        [Test]
        public async void CollectonTakeShouldBeAbleToCancel()
        {
            var cancelSource = new CancellationTokenSource();
            var collection = new NagleBlockingCollection<int>(100);

            Task.Delay(TimeSpan.FromMilliseconds(100)).ContinueWith(t => cancelSource.Cancel());

            var sw = Stopwatch.StartNew();
            var data = await collection.TakeBatch(10, TimeSpan.FromMilliseconds(500), cancelSource.Token);
            sw.Stop();

            Assert.That(sw.ElapsedMilliseconds, Is.LessThan(300));
        }

        [Test]
        public async void TakeBatchShouldRemoveItemsFromCollection()
        {
            const int expectedCount = 10;

            var collection = new NagleBlockingCollection<int>(100);
            await collection.AddRangeAsync(Enumerable.Range(0, expectedCount), CancellationToken.None);

            var data = await collection.TakeBatch(expectedCount, TimeSpan.FromMilliseconds(100), CancellationToken.None);

            Assert.That(data.Count, Is.EqualTo(expectedCount));
            Assert.That(collection.Count, Is.EqualTo(0));
        }

        [Test]
        public async void CollectionShouldWaitXForBatchSizeToCollect()
        {
            const int expectedDelay = 100;
            const int expectedCount = 10;

            var collection = new NagleBlockingCollection<int>(100);
            await collection.AddRangeAsync(Enumerable.Range(0, expectedCount), CancellationToken.None);

            var sw = Stopwatch.StartNew();
            var data = await collection.TakeBatch(expectedCount + 1, TimeSpan.FromMilliseconds(expectedDelay), CancellationToken.None);

            Assert.That(sw.ElapsedMilliseconds, Is.GreaterThanOrEqualTo(expectedDelay));
            Assert.That(data.Count, Is.EqualTo(expectedCount));
        }

        [Test]
        public async void CollectionShouldReportCorrectBufferCount()
        {
            var collection = new NagleBlockingCollection<int>(100);

            var dataTask = collection.TakeBatch(10, TimeSpan.FromSeconds(5), CancellationToken.None);

            await collection.AddRangeAsync(Enumerable.Range(0, 9), CancellationToken.None);
            Assert.That(collection.Count, Is.EqualTo(9));

            collection.AddAsync(1, CancellationToken.None);
            var data = await dataTask;
            Assert.That(data.Count, Is.EqualTo(10));
            Assert.That(collection.Count, Is.EqualTo(0));
        }

        [Test]
        public async void TakeAsyncShouldReturnAsSoonAsBatchSizeArrived()
        {
            var collection = new NagleBlockingCollection<int>(100);

            var dataTask = collection.TakeBatch(10, TimeSpan.FromSeconds(5), CancellationToken.None);

            collection.AddRangeAsync(Enumerable.Range(0, 10), CancellationToken.None);

            await dataTask;

            Assert.That(collection.Count, Is.EqualTo(0));

        }

        [Test]
        public async void TakeAsyncShouldReturnEvenWhileMoreDataArrives()
        {
            var exit = false;
            var collection = new NagleBlockingCollection<int>(10000000);

            var sw = Stopwatch.StartNew();
            var dataTask = collection.TakeBatch(10, TimeSpan.FromMilliseconds(5000), CancellationToken.None);


            var highVolumeAdding = Task.Factory.StartNew(async () =>
            {
                //high volume of data adds
                while (exit == false)
                {
                    await collection.AddAsync(1, CancellationToken.None);
                    Thread.Sleep(5);
                }
            });

            Console.WriteLine("Awaiting data...");
            await dataTask;

            Assert.That(dataTask.Result.Count, Is.EqualTo(10));
            Assert.That(sw.ElapsedMilliseconds, Is.LessThan(5000));
            exit = true;

            Console.WriteLine("Waiting to unwind test...");
            await highVolumeAdding;
        }


        [Test]
        public void TakeAsyncShouldPlayNiceWithTPL()
        {
            const int expected = 200;
            const int max = 400;
            var exit = false;
            var collection = new NagleBlockingCollection<int>(10000000);

            var dataTask = collection.TakeBatch(expected, TimeSpan.FromSeconds(100), CancellationToken.None);

            dataTask.ContinueWith(x => exit = true);

            Parallel.ForEach(Enumerable.Range(0, max),
                   new ParallelOptions { MaxDegreeOfParallelism = 20 },
                   async x =>
                   {
                       while (exit == false)
                       {
                           await collection.AddAsync(x, CancellationToken.None);
                           Thread.Sleep(100);
                       }
                   });

            Console.WriteLine("Left in collection: {0}", collection.Count);
            Assert.That(dataTask.Result.Count, Is.EqualTo(expected));
            Assert.That(collection.Count, Is.LessThan(max - expected));
        }

        [Test]
        public void TakeAsyncShouldBeThreadSafe()
        {
            const int expected = 10;
            const int max = 100;
            var exit = false;
            var collection = new NagleBlockingCollection<int>(10000000);

            var take1 = collection.TakeBatch(expected, TimeSpan.FromSeconds(100), CancellationToken.None);
            var take2 = collection.TakeBatch(expected, TimeSpan.FromSeconds(100), CancellationToken.None);
            var take3 = collection.TakeBatch(expected, TimeSpan.FromSeconds(100), CancellationToken.None);

            take1.ContinueWith(t => Console.WriteLine("Take1 done..."));
            take2.ContinueWith(t => Console.WriteLine("Take2 done..."));
            take3.ContinueWith(t => Console.WriteLine("Take3 done..."));
            Task.WhenAll(take1, take2, take3).ContinueWith(x => exit = true);

            Parallel.ForEach(Enumerable.Range(0, max),
                   new ParallelOptions { MaxDegreeOfParallelism = 20 },
                   async x =>
                   {
                       while (exit == false)
                       {
                           await collection.AddAsync(x, CancellationToken.None);
                           Thread.Sleep(100);
                       }
                   });

            Console.WriteLine("Left in collection: {0}", collection.Count);
            Assert.That(take1.Result.Count, Is.EqualTo(expected));
            Assert.That(take2.Result.Count, Is.EqualTo(expected));
            Assert.That(take3.Result.Count, Is.EqualTo(expected));
            Assert.That(collection.Count, Is.LessThan(max - (expected*3)));
        }

        #region Stop/Dispose Tests...

        [Test]
        public void StoppingCollectionShouldMarkAsComplete()
        {
            var collection = new NagleBlockingCollection<int>(100);
            using (collection)
            {
                Assert.That(collection.IsCompleted, Is.False);
                collection.CompleteAdding();
                Assert.That(collection.IsCompleted, Is.True);
            }
        }

        [Test]
        [ExpectedException(typeof(ObjectDisposedException))]
        public async void StoppingCollectionShouldPreventMoreItemsAdded()
        {
            var collection = new NagleBlockingCollection<int>(100);
            using (collection)
            {
                await collection.AddAsync(1, CancellationToken.None);
                Assert.That(collection.Count, Is.EqualTo(1));
                collection.CompleteAdding();
                await collection.AddAsync(1, CancellationToken.None);
            }
        }

        [Test]
        [ExpectedException(typeof(ObjectDisposedException))]
        public async void DisposingCollectionShouldPreventMoreItemsAdded()
        {
            var collection = new NagleBlockingCollection<int>(100);
            using (collection)
            {
                await collection.AddAsync(1, CancellationToken.None);
            }

            Assert.That(collection.Count, Is.EqualTo(1));
            await collection.AddAsync(1, CancellationToken.None);
        }

        [Test]
        public void CollectionShouldDisposeWithoutExceptions()
        {
            using (var collection = new NagleBlockingCollection<int>(100))
            {
                
            }

        }

        [Test]
        public void CollectionShouldDisposeEvenWithQueueAddAsync()
        {
            var addTasks = new List<Task>();
            using (var collection = new NagleBlockingCollection<int>(1))
            {
                addTasks.Add(collection.AddAsync(1, CancellationToken.None));
                addTasks.Add(collection.AddAsync(1, CancellationToken.None));
                addTasks.Add(collection.AddAsync(1, CancellationToken.None));
            }

            Assert.That(addTasks[0].IsCompleted);
            Assert.False(addTasks[1].IsCompleted);
            Assert.False(addTasks[2].IsCompleted);
        }


        #endregion
    }
}
