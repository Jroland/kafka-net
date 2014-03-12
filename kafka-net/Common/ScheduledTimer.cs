using System;
using System.Timers;

namespace KafkaNet.Common
{
    public enum ScheduledTimerStatus
    {
        /// <summary>
        ///     Timer is stopped.
        /// </summary>
        Stopped,

        /// <summary>
        ///     Timer is running.
        /// </summary>
        Running
    }

    public interface IScheduledTimer : IDisposable
    {
        /// <summary>
        ///     Current running status of the timer.
        /// </summary>
        ScheduledTimerStatus Status { get; }

        /// <summary>
        /// Indicates if the timer is running.
        /// </summary>
        bool Enabled { get; }

        /// <summary>
        ///     Set the time to start a replication.
        /// </summary>
        /// <param name="start">Start date and time for the replication timer.</param>
        /// <returns>Instance of ScheduledTimer for fluent configuration.</returns>
        /// <remarks>If no interval is set, the replication will only happen once.</remarks>
        IScheduledTimer StartingAt(DateTime start);

        /// <summary>
        ///     Set the interval to send a replication command to a Solr server.
        /// </summary>
        /// <param name="interval">Interval this command is to be called.</param>
        /// <returns>Instance of ScheduledTimer for fluent configuration.</returns>
        /// <remarks>If no start time is set, the interval starts when the timer is started.</remarks>
        IScheduledTimer Every(TimeSpan interval);

        /// <summary>
        ///     Action to perform when the timer expires.
        /// </summary>
        IScheduledTimer Do(Action action);

        /// <summary>
        /// Sets the timer to execute and restart the timer without waiting for the Do method to finish.
        /// </summary>
        /// <returns></returns>
        IScheduledTimer DontWait();

        /// <summary>
        ///     Starts the timer
        /// </summary>
        IScheduledTimer Begin();

        /// <summary>
        ///     Stop the timer.
        /// </summary>
        IScheduledTimer End();
    }

    /// <summary>
    /// TODO there is a bug in this that sometimes calls the do function twice on startup
    /// Timer class which providers a fluent interface for scheduling task for threads to execute at some future point.
    /// 
    /// Thanks goes to Jeff Vanzella for this nifty little fluent class for scheduling tasks.
    /// </summary>
    public class ScheduledTimer : IScheduledTimer
    {
        private bool _disposed;
        private readonly Timer _timer;
        private TimeSpan? _interval;
        private DateTime? _timerStart;
        private Action _action;
        private bool _dontWait;

        /// <summary>
        ///     Current running status of the timer.
        /// </summary>
        public ScheduledTimerStatus Status { get; private set; }

        /// <summary>
        ///     Constructor.
        /// </summary>
        public ScheduledTimer()
        {
            _interval = null;
            _timerStart = null;

            _timer = new Timer();
            _timer.Elapsed += ReplicationStartupTimerElapsed;
            _timer.AutoReset = true;

            Status = ScheduledTimerStatus.Stopped;
        }

        private void WaitActionWrapper()
        {
            if (_action == null) return;

            try
            {
                _timer.Enabled = false;
                _action();
            }
            finally
            {
                if (_disposed == false)
                    _timer.Enabled = true;
            }
        }

        private void ReplicationTimerElapsed(object sender, ElapsedEventArgs e)
        {
            if (_action == null) return;

            if (_dontWait)
                _action();
            else
                WaitActionWrapper();
        }

        private void ReplicationStartupTimerElapsed(object sender, ElapsedEventArgs e)
        {
            if (_interval.HasValue)
            {
                _timer.Stop();

                _timer.Elapsed -= ReplicationStartupTimerElapsed;
                _timer.Elapsed += ReplicationTimerElapsed;

                _timer.Interval = ProcessIntervalAndEnsureItIsGreaterThan0(_interval.Value);

                _timer.Start();
            }
            else
            {
                End();
            }

            ReplicationTimerElapsed(sender, e);
        }

        /// <summary>
        /// Indicates if the timer is running.
        /// </summary>
        public bool Enabled { get { return _timer.Enabled; } }

        /// <summary>
        ///     Set the time to start the first execution of the scheduled task.
        /// </summary>
        /// <param name="start">Start date and time for the replication timer.</param>
        /// <returns>Instance of IScheduledTimer for fluent configuration.</returns>
        /// <remarks>If no start time is set, the interval starts when the timer is started.</remarks>
        public IScheduledTimer StartingAt(DateTime start)
        {
            _timerStart = start;

            _timer.Interval = ProcessIntervalAndEnsureItIsGreaterThan0(start - DateTime.Now);

            return this;
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            if (Status == ScheduledTimerStatus.Running)
            {
                End();
            }

            using (_timer)
            {
                _disposed = true;
            }
        }

        /// <summary>
        ///     Set the interval to wait between each execution of the task.
        /// </summary>
        /// <param name="interval">Interval to wait between execution of tasks.</param>
        /// <returns>Instance of IScheduledTimer for fluent configuration.</returns>
        /// <remarks>If no interval is set, the schedule will only execute once.</remarks>
        public IScheduledTimer Every(TimeSpan interval)
        {
            _interval = interval;

            if (!_timerStart.HasValue)
            {
                _timer.Interval = ProcessIntervalAndEnsureItIsGreaterThan0(_interval.Value);
            }

            return this;
        }

        /// <summary>
        ///     Action to perform when the timer expires.
        /// </summary>
        public IScheduledTimer Do(Action action)
        {
            _action = action;

            return this;
        }

        /// <summary>
        /// Sets the timer to execute and restart the timer without waiting for the Do method to finish.
        /// </summary>
        /// <remarks>
        /// Setting this will start the countdown timer to the next execution imediately 
        /// after the current execution is triggered.  If the execution action takes longer than
        /// the timer interval, the execution task will stack and run concurrently.
        /// </remarks>
        public IScheduledTimer DontWait()
        {
            _dontWait = true;
            return this;
        }

        private static double ProcessIntervalAndEnsureItIsGreaterThan0(TimeSpan interval)
        {
            var intervalInMilliseconds = interval.TotalMilliseconds;

            intervalInMilliseconds =
                (intervalInMilliseconds < 1)
                    ? 1
                    : intervalInMilliseconds;

            return intervalInMilliseconds;
        }

        /// <summary>
        ///     Starts the timer
        /// </summary>
        public IScheduledTimer Begin()
        {
            if (!_timerStart.HasValue)
            {
                StartingAt(DateTime.Now);
            }

            Status = ScheduledTimerStatus.Running;

            _timer.Enabled = true;

            return this;
        }

        /// <summary>
        ///     Stop the timer.
        /// </summary>
        public IScheduledTimer End()
        {
            Status = ScheduledTimerStatus.Stopped;
            _timer.Enabled = false;

            return this;
        }

        /// <summary>
        /// Exposes the timer object for unit testing.
        /// </summary>
        public Timer TimerObject
        {
            get { return _timer; }
        }
    }
}
