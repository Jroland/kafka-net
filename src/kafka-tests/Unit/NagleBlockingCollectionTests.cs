using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet.Common;
using kafka_tests.Helpers;
using NSubstitute.Core;
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

            var addTask = Task.Factory.StartNew(() => collection.AddRangeAsync(Enumerable.Range(0, blockingCount + expectedCount)).Wait());

            TaskTest.WaitFor(() => collection.Count >= blockingCount);

            Assert.That(collection.Count, Is.EqualTo(blockingCount), "The collection should only contain 10 items.");
            Assert.That(addTask.Status, Is.EqualTo(TaskStatus.Running), "The task should be blocking.");

            //unblock the collection
            await collection.TakeBatch(blockingCount, TimeSpan.FromMilliseconds(100));

            await addTask;

            Assert.That(collection.Count, Is.EqualTo(expectedCount));
            Assert.That(addTask.Status, Is.EqualTo(TaskStatus.RanToCompletion));
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
            await collection.AddRangeAsync(Enumerable.Range(0, expectedCount));

            var data = await collection.TakeBatch(expectedCount, TimeSpan.FromMilliseconds(100));

            Assert.That(data.Count, Is.EqualTo(expectedCount));
            Assert.That(collection.Count, Is.EqualTo(0));
        }

        [Test]
        public async void CollectionShouldWaitXForBatchSizeToCollect()
        {
            const int expectedDelay = 100;
            const int expectedCount = 10;

            var collection = new NagleBlockingCollection<int>(100);
            await collection.AddRangeAsync(Enumerable.Range(0, expectedCount));

            var sw = Stopwatch.StartNew();
            var data = await collection.TakeBatch(expectedCount + 1, TimeSpan.FromMilliseconds(expectedDelay));

            Assert.That(sw.ElapsedMilliseconds, Is.GreaterThanOrEqualTo(expectedDelay));
            Assert.That(data.Count, Is.EqualTo(expectedCount));
        }

        [Test]
        public async void CollectionShouldReportCorrectBufferCount()
        {
            var collection = new NagleBlockingCollection<int>(100);

            var dataTask = collection.TakeBatch(10, TimeSpan.FromSeconds(5));

            await collection.AddRangeAsync(Enumerable.Range(0, 9));
            Assert.That(collection.Count, Is.EqualTo(9));

            collection.AddAsync(1);
            var data = await dataTask;
            Assert.That(data.Count, Is.EqualTo(10));
            Assert.That(collection.Count, Is.EqualTo(0));
        }

        [Test]
        public async void TakeAsyncShouldReturnAsSoonAsBatchSizeArrived()
        {
            var collection = new NagleBlockingCollection<int>(100);

            var dataTask = collection.TakeBatch(10, TimeSpan.FromSeconds(5));

            collection.AddRangeAsync(Enumerable.Range(0, 10));

            await dataTask;

            Assert.That(collection.Count, Is.EqualTo(0));

        }

        [Test]
        public async void TakeAsyncShouldReturnEvenWhileMoreDataArrives()
        {
            var exit = false;
            var collection = new NagleBlockingCollection<int>(10000000);

            var sw = Stopwatch.StartNew();
            var dataTask = collection.TakeBatch(10, TimeSpan.FromMilliseconds(5000));


            var highVolumeAdding = Task.Factory.StartNew(async () =>
            {
                //high volume of data adds
                while (exit == false)
                {
                    await collection.AddAsync(1);
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
        public async void TakeAsyncShouldPlayNiceWithTPL()
        {
            const int expected = 50;
            const int max = 100;
            var exit = false;
            var collection = new NagleBlockingCollection<int>(10000000);

            var sw = Stopwatch.StartNew();
            var dataTask = collection.TakeBatch(expected, TimeSpan.FromMilliseconds(5000));

            var tplAddingTask = Task.Factory.StartNew(() =>
            {
                Parallel.ForEach(Enumerable.Range(0, max),
                    new ParallelOptions {MaxDegreeOfParallelism = 20},
                    x =>
                    {
                        while (exit == false)
                        {
                            collection.AddAsync(x).Wait();
                            Console.WriteLine("Added {0}", x);
                            Thread.Sleep(100);
                        }
                    });
            });

            Console.WriteLine("Awaiting data...");
            await dataTask;

            Assert.That(dataTask.Result.Count, Is.EqualTo(expected));
            Assert.That(sw.ElapsedMilliseconds, Is.LessThan(5000));
            Assert.That(collection.Count, Is.LessThan(max - expected));
            Console.WriteLine("Left in collection: {0}", collection.Count);
            exit = true;

            Console.WriteLine("Waiting to unwind test...");
            await tplAddingTask;
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
                await collection.AddAsync(1);
                Assert.That(collection.Count, Is.EqualTo(1));
                collection.CompleteAdding();
                await collection.AddAsync(1);
            }
        }

        [Test]
        [ExpectedException(typeof(ObjectDisposedException))]
        public async void DisposingCollectionShouldPreventMoreItemsAdded()
        {
            var collection = new NagleBlockingCollection<int>(100);
            using (collection)
            {
                await collection.AddAsync(1);
            }

            Assert.That(collection.Count, Is.EqualTo(1));
            await collection.AddAsync(1);
        }

        #endregion

    }
}
