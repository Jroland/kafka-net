using System;
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
        public async void OnDataAvailableWithTimeoutShouldCancelAndReturnFalse()
        {
            var aq = new AsyncCollection<bool>();
            var cancelToken = new CancellationTokenSource();
            Task.Delay(TimeSpan.FromMilliseconds(100)).ContinueWith(t => cancelToken.Cancel());

            var result = await aq.OnDataAvailable(TimeSpan.FromSeconds(10), cancelToken.Token);

            Assert.That(result, Is.EqualTo(false), "Cancelled on data available should return false.");
        }

        [Test]
        public async void OnDataAvailableWithTimeoutShouldReturnFalse()
        {
            var aq = new AsyncCollection<bool>();

            var result = await aq.OnDataAvailable(TimeSpan.FromMilliseconds(10), CancellationToken.None);

            Assert.That(result, Is.False, "Timeout call should return false.");
        }

        [Test]
        public async void OnDataAvailableWithNoTimeoutShouldReturnTrue()
        {
            var aq = new AsyncCollection<bool>();
            Task.Delay(TimeSpan.FromMilliseconds(100)).ContinueWith(t => aq.Add(true));
            var result = await aq.OnDataAvailable(TimeSpan.FromSeconds(1), CancellationToken.None);

            Assert.That(result, Is.True, "Should return true when data arrives before timeout.");
        }
    }
}
