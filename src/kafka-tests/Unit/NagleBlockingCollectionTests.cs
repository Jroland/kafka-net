using System;
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

            var addTask = Task.Factory.StartNew(() => collection.AddRange(Enumerable.Range(0, blockingCount + expectedCount)));

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
        [ExpectedException(typeof(OperationCanceledException))]
        public async void CollectonTakeShouldBeAbleToCancel()
        {
            var cancelSource = new CancellationTokenSource();
            var collection = new NagleBlockingCollection<int>(100);

            Task.Delay(TimeSpan.FromMilliseconds(100)).ContinueWith(t => cancelSource.Cancel());

            var data = await collection.TakeBatch(10, TimeSpan.FromMilliseconds(500), cancelSource.Token);
        }

        [Test]
        public async void TakeBatchShouldRemoveItemsFromCollection()
        {
            const int expectedCount = 10;

            var collection = new NagleBlockingCollection<int>(100);
            collection.AddRange(Enumerable.Range(0, expectedCount));

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
            collection.AddRange(Enumerable.Range(0, expectedCount));

            var sw = Stopwatch.StartNew();
            var data = await collection.TakeBatch(expectedCount + 1, TimeSpan.FromMilliseconds(expectedDelay));

            Assert.That(sw.ElapsedMilliseconds, Is.GreaterThanOrEqualTo(expectedDelay));
            Assert.That(data.Count, Is.EqualTo(expectedCount));
        }

        [Test]
        public void DisposingCollectionShouldMarkAsComplete()
        {
            var collection = new NagleBlockingCollection<int>(100);
            using(collection)
            {
                Assert.That(collection.IsComplete, Is.False);
            }

            Assert.That(collection.IsComplete, Is.True);
        }

        [Test]
        [ExpectedException(typeof(ObjectDisposedException))]
        public void DisposingCollectionShouldPreventMoreItemsAdded()
        {
            var collection = new NagleBlockingCollection<int>(100);
            using (collection)
            {
                collection.Add(1);
            }

            Assert.That(collection.Count, Is.EqualTo(1));
            collection.Add(1);
        }
    }
}
