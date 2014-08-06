using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaNet.Common
{
    /// <summary>
    /// The initial state of the ThreadWall when the class is constructed.
    /// </summary>
    public enum ThreadWallState { Blocked, UnBlocked }

    /// <summary>
    /// Provides a non-exclusive lock on a section of code.  The idea here is to allow one thread to inform all other
    /// threads that a section of code is being worked on.  When the working thread is completed, all other threads 
    /// should be allowed past the wall at the same time without lock contention between the other threads.  The waiting
    /// threads are able to async await the passage through the wall.
    /// 
    /// TODO There is almost certainly a better way to handle this type of senario, research/suggestions?
    /// </summary>
    public class ThreadWall
    {
        private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1, 1);

        public ThreadWall(ThreadWallState state)
        {
            switch (state)
            {
                case ThreadWallState.Blocked:
                    _semaphore = new SemaphoreSlim(0, 1);
                    break;
                case ThreadWallState.UnBlocked:
                    _semaphore = new SemaphoreSlim(1, 1);
                    break;
                default:
                    throw new NotSupportedException("Unsupported ThreadWallState supplied.");
            }
        }

        /// <summary>
        /// Indicates the current thread wall state and whether it will allow threads through or not.
        /// </summary>
        public ThreadWallState WallState
        {
            get
            {
                return _semaphore.CurrentCount == 0 ? ThreadWallState.Blocked : ThreadWallState.UnBlocked;
            }
        }

        /// <summary>
        /// Signal to close the door on the thread wall.  Multiple calls are ignored.
        /// </summary>
        public void Block()
        {
            //TODO this checks are not thread safe
            //if the semaphore is already blocking then ignore block request.
            if (_semaphore.CurrentCount > 0)
                _semaphore.Wait();
        }

        /// <summary>
        /// Open the door on the thread wall and allow other threads to pass.
        /// </summary>
        public void Release()
        {
            //TODO this checks are not thread safe
            //if we already have an open spot ignore release command.
            if (_semaphore.CurrentCount <= 0)
                _semaphore.Release();
        }

        /// <summary>
        /// Async request a passage through the thread wall.
        /// </summary>
        /// <returns>Task handle to signal passage allowed.</returns>
        public Task RequestPassageAsync()
        {
            return AsTask(_semaphore.AvailableWaitHandle, Timeout.InfiniteTimeSpan);
        }

        private static Task AsTask(WaitHandle handle, TimeSpan timeout)
        {
            var tcs = new TaskCompletionSource<object>();
            var registration = ThreadPool.RegisterWaitForSingleObject(handle, (state, timedOut) =>
            {
                var localTcs = (TaskCompletionSource<object>)state;
                if (timedOut)
                    localTcs.TrySetCanceled();
                else
                    localTcs.TrySetResult(null);
            }, tcs, timeout, executeOnlyOnce: true);
            tcs.Task.ContinueWith((_, state) => ((RegisteredWaitHandle)state).Unregister(null), registration, TaskScheduler.Default);
            return tcs.Task;
        }
    }
}
