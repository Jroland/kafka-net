﻿using kafka_tests.Helpers;
using KafkaNet.Common;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace kafka_tests.Unit
{
    [TestFixture]
    public class AsyncCollectionTests
    {
        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void AsyncCollectionConstructs()
        {
            var collection = new AsyncCollection<string>();
            Assert.That(collection, Is.Not.Null);
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void OnDataAvailableShouldTriggerWhenDataAdded()
        {
            var aq = new AsyncCollection<bool>();

            Assert.That(aq.OnHasDataAvailable(CancellationToken.None).IsCompleted, Is.False, "Task should indicate no data available.");

            aq.Add(true);

            Assert.That(aq.OnHasDataAvailable(CancellationToken.None).IsCompleted, Is.True, "Task should indicate data available.");
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void OnDataAvailableShouldBlockWhenDataRemoved()
        {
            var aq = new AsyncCollection<bool>();

            aq.Add(true);

            Assert.That(aq.OnHasDataAvailable(CancellationToken.None).IsCompleted, Is.True, "Task should indicate data available.");

            bool data;
            aq.TryTake(out data);
            Assert.That(aq.OnHasDataAvailable(CancellationToken.None).IsCompleted, Is.False, "Task should indicate no data available.");
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task OnDataAvailableShouldCancel()//change class Behavers from trowing OperationCanceledException to returning false
        {//do not debug with brack point it harm this test
            var aq = new AsyncCollection<bool>();
            int timeSpen = 100;
            Task waitUntilCancel = aq.OnHasDataAvailable(new CancellationTokenSource(timeSpen).Token);
            await Task.WhenAny(waitUntilCancel, Task.Delay(timeSpen / 2));
            Assert.IsFalse(waitUntilCancel.IsCompleted, "task Should Cancel only when time is up");
            //ToDO FIX
            //   Assert.IsFalse(await waitUntilCancel, "it Should return false when cancel");
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void DrainShouldBlockWhenDataRemoved()
        {
            var aq = new AsyncCollection<bool>();

            aq.Add(true);
            aq.Add(true);

            Assert.That(aq.OnHasDataAvailable(CancellationToken.None).IsCompleted, Is.True, "Task should indicate data available.");

            var drained = aq.Drain().ToList();
            Assert.That(aq.OnHasDataAvailable(CancellationToken.None).IsCompleted, Is.False, "Task should indicate no data available.");
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async void CollectionShouldReportCorrectBufferCount()
        {
            var collection = new AsyncCollection<int>();

            var dataTask = collection.TakeAsync(10, TimeSpan.FromHours(5), CancellationToken.None);

            collection.AddRange(Enumerable.Range(0, 9));
            Assert.That(collection.Count, Is.EqualTo(9));

            collection.Add(1);
            var data = await dataTask;
            Assert.That(data.Count, Is.EqualTo(10));
            Assert.That(collection.Count, Is.EqualTo(0));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [Ignore("Currently remove max buffer feature.")]
        public async Task CollectionShouldBlockOnMaxBuffer()
        {
            var collection = new AsyncCollection<int>();
            var task = Task.Factory.StartNew(() => collection.AddRange(Enumerable.Range(0, 10)));
            await TaskTest.WaitFor(() => collection.Count >= 9);
            Assert.That(collection.Count, Is.EqualTo(9), "Buffer should block at 9 items.");
            Assert.That(task.IsCompleted, Is.False, "Task should be blocking on last item.");
            var item = collection.Pop();
            await TaskTest.WaitFor(() => task.IsCompleted);
            Assert.That(task.IsCompleted, Is.True, "Task should complete after room is made in buffer.");
            Assert.That(collection.Count, Is.EqualTo(9), "There should now be 9 items in the buffer.");
        }

        #region Take Tests...

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void TryTakeShouldReturnFalseOnEmpty()
        {
            var aq = new AsyncCollection<bool>();

            Assert.That(aq.OnHasDataAvailable(CancellationToken.None).IsCompleted, Is.False, "Task should indicate no data available.");

            bool data;
            Assert.That(aq.TryTake(out data), Is.False, "TryTake should report false on empty collection.");
            Assert.That(aq.OnHasDataAvailable(CancellationToken.None).IsCompleted, Is.False, "Task should indicate no data available.");
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task TakeAsyncShouldOnlyWaitTimeoutAndReturnWhatItHas()
        {
            const int size = 20;
            var aq = new AsyncCollection<bool>();

            Task.Run(() =>
            {
                //this should take 2000ms to complete
                for (int i = 0; i < size; i++)
                {
                    aq.Add(true);
                    Thread.Sleep(100);
                }
            });

            var result = await aq.TakeAsync(size, TimeSpan.FromMilliseconds(100), CancellationToken.None);

            Assert.That(result.Count, Is.LessThan(size));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async void TakeAsyncShouldReturnEmptyListIfNothingFound()
        {
            var aq = new AsyncCollection<bool>();

            var result = await aq.TakeAsync(100, TimeSpan.FromMilliseconds(100), CancellationToken.None).ConfigureAwait(false);

            Assert.That(result, Is.Not.Null);
            Assert.That(result.Count, Is.EqualTo(0));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task TakeAsyncShouldBeAbleToCancel()
        {
            var cancelSource = new CancellationTokenSource();
            var collection = new AsyncCollection<int>();

            Task.Delay(TimeSpan.FromMilliseconds(100)).ContinueWith(t => cancelSource.Cancel());

            var sw = Stopwatch.StartNew();
            var data = await collection.TakeAsync(10, TimeSpan.FromMilliseconds(500), cancelSource.Token);
            sw.Stop();

            Assert.That(sw.ElapsedMilliseconds, Is.LessThan(300));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async void TakeAsyncShouldRemoveItemsFromCollection()
        {
            const int expectedCount = 10;

            var collection = new AsyncCollection<int>();
            collection.AddRange(Enumerable.Range(0, expectedCount));

            var data = await collection.TakeAsync(expectedCount, TimeSpan.FromMilliseconds(100), CancellationToken.None);

            Assert.That(data.Count, Is.EqualTo(expectedCount));
            Assert.That(collection.Count, Is.EqualTo(0));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async void TakeAsyncShouldWaitXForBatchSizeToCollect()
        {
            const int expectedDelay = 100;
            const int expectedCount = 10;

            var collection = new AsyncCollection<int>();
            collection.AddRange(Enumerable.Range(0, expectedCount));

            var sw = Stopwatch.StartNew();
            var data = await collection.TakeAsync(expectedCount + 1, TimeSpan.FromMilliseconds(expectedDelay), CancellationToken.None);

            Assert.That(sw.ElapsedMilliseconds, Is.GreaterThanOrEqualTo(expectedDelay));
            Assert.That(data.Count, Is.EqualTo(expectedCount));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async void TakeAsyncShouldReturnAsSoonAsBatchSizeArrived()
        {
            var collection = new AsyncCollection<int>();

            var dataTask = collection.TakeAsync(10, TimeSpan.FromSeconds(5), CancellationToken.None);

            collection.AddRange(Enumerable.Range(0, 10));

            await dataTask;

            Assert.That(collection.Count, Is.EqualTo(0));
        }

        #endregion Take Tests...

        #region Thread Contention Tests...

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async void TakeAsyncShouldReturnEvenWhileMoreDataArrives()
        {
            var exit = false;
            var collection = new AsyncCollection<int>();

            var sw = Stopwatch.StartNew();
            var dataTask = collection.TakeAsync(10, TimeSpan.FromMilliseconds(5000), CancellationToken.None);

            var highVolumeAdding = Task.Run(() =>
            {
                //high volume of data adds
                while (exit == false)
                {
                    collection.Add(1);
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
            collection.Drain();
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void TakeAsyncShouldPlayNiceWithTPL()
        {
            const int expected = 200;
            const int max = 400;
            var exit = false;
            var collection = new AsyncCollection<int>();

            var dataTask = collection.TakeAsync(expected, TimeSpan.FromSeconds(100), CancellationToken.None);

            dataTask.ContinueWith(x => exit = true);

            Parallel.ForEach(Enumerable.Range(0, max).ToList(),
                   new ParallelOptions { MaxDegreeOfParallelism = 20 },
                   x =>
                   {
                       while (exit == false)
                       {
                           collection.Add(x);
                           Thread.Sleep(100);
                       }
                   });

            Console.WriteLine("Left in collection: {0}", collection.Count);
            Assert.That(dataTask.Result.Count, Is.EqualTo(expected));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void TakeAsyncShouldBeThreadSafe()
        {
            const int expected = 10;
            const int max = 100;
            var exit = false;
            var collection = new AsyncCollection<int>();

            var take1 = collection.TakeAsync(expected, TimeSpan.FromSeconds(100), CancellationToken.None);
            var take2 = collection.TakeAsync(expected, TimeSpan.FromSeconds(100), CancellationToken.None);
            var take3 = collection.TakeAsync(expected, TimeSpan.FromSeconds(100), CancellationToken.None);

            take1.ContinueWith(t => Console.WriteLine("Take1 done..."));
            take2.ContinueWith(t => Console.WriteLine("Take2 done..."));
            take3.ContinueWith(t => Console.WriteLine("Take3 done..."));
            Task.WhenAll(take1, take2, take3).ContinueWith(x => exit = true);

            Parallel.ForEach(Enumerable.Range(0, max).ToList(),
                   new ParallelOptions { MaxDegreeOfParallelism = 20 },
                   x =>
                   {
                       while (exit == false)
                       {
                           collection.Add(x);
                           Thread.Sleep(100);
                       }
                   });

            Console.WriteLine("Left in collection: {0}", collection.Count);
            Assert.That(take1.Result.Count, Is.EqualTo(expected));
            Assert.That(take2.Result.Count, Is.EqualTo(expected));
            Assert.That(take3.Result.Count, Is.EqualTo(expected));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void AddRangeShouldBePerformant()
        {
            AsyncCollection<int> collection = new AsyncCollection<int>();
            var sw = Stopwatch.StartNew();
            collection.AddRange(Enumerable.Range(0, 1000000));
            sw.Stop();
            Console.WriteLine("Performance: {0}", sw.ElapsedMilliseconds);
            Assert.That(sw.ElapsedMilliseconds, Is.LessThan(200));
        }

        [TearDown]
        public void Cleanup()
        {
            GC.Collect();
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async void TakeAsyncShouldBePerformant()
        {
            const int dataSize = 1000000;
            AsyncCollection<int> collection = new AsyncCollection<int>();

            collection.AddRange(Enumerable.Range(0, dataSize));
            var sw = Stopwatch.StartNew();
            var list = await collection.TakeAsync(dataSize, TimeSpan.FromSeconds(1), CancellationToken.None);
            sw.Stop();
            Console.WriteLine("Performance: {0}", sw.ElapsedMilliseconds);
            Assert.That(list.Count, Is.EqualTo(dataSize));
            Assert.That(sw.ElapsedMilliseconds, Is.LessThan(200));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public async Task AddAndRemoveShouldBePerformant()
        {
            AsyncCollection<int> collection = new AsyncCollection<int>();

            const int dataSize = 1000000;
            List<int> receivedData = null;// new List<int>(dataSize);
            var sw = Stopwatch.StartNew();

            var t = Task.Run(() => collection.AddRange(Enumerable.Range(0, dataSize)));
            var t2 = Task.Run(() => receivedData = collection.TakeAsync(dataSize, TimeSpan.FromSeconds(5), CancellationToken.None).Result);
            await Task.WhenAll(t, t2);
            sw.Stop();
            Console.WriteLine("Performance: {0}", sw.ElapsedMilliseconds);
            Assert.That(receivedData.Count, Is.EqualTo(dataSize));
            Assert.That(sw.ElapsedMilliseconds, Is.LessThan(200));
        }

        #endregion Thread Contention Tests...

        #region CompletedTests Tests...

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [ExpectedException(typeof(ObjectDisposedException))]
        public void CompletedCollectionShouldPreventMoreItemsAdded()
        {
            var collection = new AsyncCollection<int>();
            collection.Add(1);
            collection.CompleteAdding();
            collection.Add(1);
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void CompletedCollectionShouldShowCompletedTrue()
        {
            var collection = new AsyncCollection<int>();
            collection.Add(1);
            Assert.That(collection.IsCompleted, Is.False);
            collection.CompleteAdding();
            Assert.That(collection.IsCompleted, Is.True);
        }

        #endregion CompletedTests Tests...
    }
}