using System;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet.Common;
using NUnit.Framework;

namespace kafka_tests.Unit
{
    [TestFixture]
    [Category("Local")]
    public class ScheduledTimerFixture
    {
        [Test]
        public void CreateInstance()
        {
            var sut = new ScheduledTimer();

            Assert.That(sut, Is.Not.Null);
        }

        [Test]
        public void StatusShouldBeStoppedWhenInstanceCreated()
        {
            const ScheduledTimerStatus expected = ScheduledTimerStatus.Stopped;

            var sut = new ScheduledTimer();

            Assert.That(sut.Status, Is.EqualTo(expected));
        }

        [Test]
        public void StatusShouldBeStoppedWhenFluentDoesNotSpecifyBegin()
        {
            int count = 0;
            const ScheduledTimerStatus expected = ScheduledTimerStatus.Stopped;

            var sut = new ScheduledTimer()
                .Do(() => Interlocked.Increment(ref count))
                .StartingAt(DateTime.Now)
                .Every(TimeSpan.FromMilliseconds(10));

            Thread.Sleep(100);
            Assert.That(sut.Status, Is.EqualTo(expected));
            Assert.That(count, Is.EqualTo(0));
        }

        [Test]
        public void ScheduleTimerShouldOnlyCallDoOnceEvenWithMultipleBeginRequests()
        {
            int count = 0;

            var sut = new ScheduledTimer()
                .Do(() => Interlocked.Increment(ref count))
                .StartingAt(DateTime.Now)
                .Every(TimeSpan.FromMilliseconds(1000));

            Task.Run(() => sut.Begin());
            Task.Run(() => sut.Begin());
            Task.Run(() => sut.Begin());
                

            Thread.Sleep(200);
            Assert.That(count, Is.EqualTo(1));
        }

        [Test]
        public void StartWithNoParameterShouldUpdateTheStatusToRunning()
        {
            const ScheduledTimerStatus expected = ScheduledTimerStatus.Running;

            var sut = new ScheduledTimer();

            sut.Begin();

            Assert.That(sut.Status, Is.EqualTo(expected));
        }

        [Test]
        public void StartThenStopShouldUpdateStatusToStopped()
        {
            const ScheduledTimerStatus expectedRunning = ScheduledTimerStatus.Running;
            const ScheduledTimerStatus expectedStopped = ScheduledTimerStatus.Stopped;

            var sut = new ScheduledTimer();

            sut.Begin();

            Assert.That(sut.Status, Is.EqualTo(expectedRunning));

            sut.End();

            Assert.That(sut.Status, Is.EqualTo(expectedStopped));
        }

        [Test]
        public void ObjectCreationShouldCreateTheTimerObject()
        {
            var sut = new ScheduledTimer();

            Assert.That(sut.TimerObject, Is.Not.Null);
        }

        [Test]
        public void IntervalShouldBeSetTo1WhenStartWithNoParameterIsCalled()
        {
            var sut = new ScheduledTimer();

            sut.Begin();

            Assert.That(sut.TimerObject.Interval, Is.EqualTo(1));
        }
        
        [Test]
        public void SetReplicationIntervalShouldUpdateTheTimerIntervalAndAutoResetAccordingly()
        {
            var sut = new ScheduledTimer();

            var counter = 0;
            sut.Do(() => Interlocked.Increment(ref  counter)).Every(TimeSpan.FromMilliseconds(100));

            Assert.That(sut.TimerObject.Interval, Is.EqualTo(100));

            sut.Begin();

            Thread.Sleep(550);

            Assert.That(counter, Is.GreaterThanOrEqualTo(5));
        }

        [Test]
        public void SetStartTimeShouldUpdateTheTimerIntervalAccordingly()
        {
            var sut = new ScheduledTimer();

            sut.StartingAt(DateTime.Now.AddSeconds(3));

            // Becuase of the nature of time, give the interval a second of leeway
            Assert.That(sut.TimerObject.Interval, Is.InRange(2900d, 3000d));
        }

        [Test]
        public void SettingStartTimeShouldOverwriteIntervalPreviouslySet()
        {
            var sut = new ScheduledTimer();

            sut.Every(new TimeSpan(0, 0, 0, 1));

            Assert.That(sut.TimerObject.Interval, Is.EqualTo(1000));

            sut.StartingAt(DateTime.Now.AddSeconds(3));

            // Becuase of the nature of time, give the interval a second of leeway
            Assert.That(sut.TimerObject.Interval, Is.InRange(2900d, 3000d));
        }

        [Test]
        public void IntervalUpdatedBeforeStartShouldChangeTheIntervalUntilStartHasBeenCalled()
        {
            var sut = new ScheduledTimer();

            sut.Every(TimeSpan.FromMilliseconds(100));

            Assert.That(sut.TimerObject.Interval, Is.EqualTo(100));

            sut.StartingAt(DateTime.Now.AddMilliseconds(200));

            // Becuase of the nature of time, give the interval a second of leeway
            Assert.That(sut.TimerObject.Interval, Is.InRange(200d, 300d));

            sut.Begin();

            Thread.Sleep(300);

            Assert.That(sut.TimerObject.Interval, Is.EqualTo(100));
        }

        [Test]
        public void IntervalUpdatedAfterStartShouldNotChangeTheIntervalUntilStartHasBeenCalled()
        {
            var sut = new ScheduledTimer();

            sut.StartingAt(DateTime.Now.AddMilliseconds(100));

            // Becuase of the nature of time, give the interval a second of leeway
            Assert.That(sut.TimerObject.Interval, Is.InRange(100d, 200d));

            sut.Every(TimeSpan.FromMilliseconds(200));

            // Becuase of the nature of time, give the interval a second of leeway
            Assert.That(sut.TimerObject.Interval, Is.InRange(100d, 200d));

            sut.Begin();

            Thread.Sleep(300);

            Assert.That(sut.TimerObject.Interval, Is.EqualTo(200));
        }

        [Test]
        public void DisposeWithTimerRunningShouldStopTimerAndDisposeInternalTimer()
        {
            const ScheduledTimerStatus expected = ScheduledTimerStatus.Stopped;

            var sut = new ScheduledTimer();

            var disposed = false;
            sut.TimerObject.Disposed += ((sender, args) => disposed = true);

            sut.Begin();

            sut.Dispose();

            Assert.That(sut.Status, Is.EqualTo(expected));
            Assert.That(disposed, Is.True);
        }

        [Test]
        public void DisposeWithTimerStoppedShouldDisposeInternalTimer()
        {
            const ScheduledTimerStatus expected = ScheduledTimerStatus.Stopped;

            var sut = new ScheduledTimer();

            var disposed = false;
            sut.TimerObject.Disposed += ((sender, args) => disposed = true);

            sut.Dispose();

            Assert.That(sut.Status, Is.EqualTo(expected));
            Assert.That(disposed, Is.True);
        }

        [Test]
        public void StartingAtShouldWaitToStart()
        {
            int count = 0;
            var sut = new ScheduledTimer()
                .Do(() => Interlocked.Add(ref count, 1))
                .Every(TimeSpan.FromMilliseconds(100))
                .StartingAt(DateTime.Now.AddMinutes(1))
                .Begin();

            Thread.Sleep(1000);
            Assert.That(count, Is.LessThanOrEqualTo(0));
        }

        [Test]
        public void TimerShouldWaitForDoMethodByDefault()
        {
            int count = 0;
            var sut = new ScheduledTimer()
                .Do(() => { Interlocked.Add(ref count, 1); Thread.Sleep(10000); })
                .Every(TimeSpan.FromMilliseconds(100))
                .StartingAt(DateTime.Now)
                .Begin();

            Thread.Sleep(1000);
            Assert.That(count, Is.EqualTo(1));
        }

        [Test]
        [Ignore("Not using this feature.")]
        public void TimerShouldNotWaitWhenSet()
        {
            int count = 0;
            var sut = new ScheduledTimer()
                .Do(() => { Interlocked.Add(ref count, 1); Thread.Sleep(10000); })
                .Every(TimeSpan.FromMilliseconds(100))
                .DontWait()
                .StartingAt(DateTime.Now)
                .Begin();

            Thread.Sleep(1000);
            Assert.That(count, Is.GreaterThan(5));
        }
    }
}