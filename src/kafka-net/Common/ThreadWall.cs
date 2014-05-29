using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaNet.Common
{
    public class ThreadWall
    {
        private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1, 1);

        public void Block()
        {
            _semaphore.Wait();
        }

        public void Release()
        {
            _semaphore.Release();
        }

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
