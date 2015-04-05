using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet.Common;
using NUnit.Framework;

namespace kafka_tests.Unit
{
    [TestFixture]
    public class AsyncCollectionTests
    {
        [Test]
        public void OnDataAvailableShouldTriggerWhenDataAdded()
        {
            var aq = new AsyncCollection<bool>();

            Assert.That(aq.OnDataAvailable(CancellationToken.None).IsCompleted, Is.False, "Task should indicate no data available.");

            aq.Add(true);

            Assert.That(aq.OnDataAvailable(CancellationToken.None).IsCompleted, Is.True, "Task should indicate data available.");
        }

        [Test]
        public void OnDataAvailableShouldBlockWhenDataRemoved()
        {
            var aq = new AsyncCollection<bool>();

            aq.Add(true);

            Assert.That(aq.OnDataAvailable(CancellationToken.None).IsCompleted, Is.True, "Task should indicate data available.");

            bool data;
            aq.TryTake(out data);
            Assert.That(aq.OnDataAvailable(CancellationToken.None).IsCompleted, Is.False, "Task should indicate no data available.");
        }

        [Test]
        public void DrainShouldBlockWhenDataRemoved()
        {
            var aq = new AsyncCollection<bool>();

            aq.Add(true);
            aq.Add(true);

            Assert.That(aq.OnDataAvailable(CancellationToken.None).IsCompleted, Is.True, "Task should indicate data available.");

            var drained = aq.Drain().ToList();
            Assert.That(aq.OnDataAvailable(CancellationToken.None).IsCompleted, Is.False, "Task should indicate no data available.");
        }

        [Test]
        public void TryTakeShouldReturnFalseOnEmpty()
        {
            var aq = new AsyncCollection<bool>();

            Assert.That(aq.OnDataAvailable(CancellationToken.None).IsCompleted, Is.False, "Task should indicate no data available.");

            bool data;
            Assert.That(aq.TryTake(out data), Is.False, "TryTake should report false on empty collection.");
            Assert.That(aq.OnDataAvailable(CancellationToken.None).IsCompleted, Is.False, "Task should indicate no data available.");
        }

        [Test]
        [ExpectedException(typeof(OperationCanceledException))]
        public async void OnDataAvailableShouldCancel()
        {
            var aq = new AsyncCollection<bool>();
            var cancelToken = new CancellationTokenSource();
            Task.Delay(TimeSpan.FromMilliseconds(100)).ContinueWith(t => cancelToken.Cancel());

            await aq.OnDataAvailable(cancelToken.Token);
        }

        [Test]
        public async void TakeAsyncShouldOnlyWaitTimeoutAndReturnWhatItHas()
        {
            const int size = 20;
            var aq = new AsyncCollection<bool>();

            Task.Factory.StartNew(() =>
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

        [Test]
        public async void TakeAsyncShouldReturnEmptyListIfNothingFound()
        {
            var aq = new AsyncCollection<bool>();

            var result = await aq.TakeAsync(100, TimeSpan.FromMilliseconds(100), CancellationToken.None).ConfigureAwait(false);

            Assert.That(result, Is.Not.Null);
            Assert.That(result.Count, Is.EqualTo(0));
        }
    }
}
