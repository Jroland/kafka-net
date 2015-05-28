using System.Threading;
using System.Threading.Tasks;

// original idea by Stephen Toub: http://blogs.msdn.com/b/pfxteam/archive/2012/02/11/10266920.aspx

namespace KafkaNet.Common
{


    /// <summary>
    /// Async version of a manual reset event.
    /// </summary>
    public sealed class AsyncManualResetEvent
    {
        private TaskCompletionSource<bool> _tcs;

        public bool IsOpen
        {
            get { return _tcs.Task.IsCompleted; }
        }

        /// <summary>
        /// Async version of a manual reset event.
        /// </summary>
        /// <param name="set">Sets whether the initial state of the event is true=open or false=blocking.</param>
        public AsyncManualResetEvent(bool set = false)
        {
            _tcs = new TaskCompletionSource<bool>();
            if (set)
            {
                _tcs.SetResult(true);
            }
        }

        /// <summary>
        /// Async wait for the manual reset event to be triggered.
        /// </summary>
        /// <returns></returns>
        public Task WaitAsync()
        {
            return _tcs.Task;
        }

        /// <summary>
        /// Set the event and complete, releasing all WaitAsync requests.
        /// </summary>
        public void Open()
        {
            _tcs.TrySetResult(true);
        }

        /// <summary>
        /// Reset the event making all WaitAsync requests block, does nothing if already reset.
        /// </summary>
        public void Close()
        {
            while (true)
            {
                var tcs = _tcs;
                if (!tcs.Task.IsCompleted || Interlocked.CompareExchange(ref _tcs, new TaskCompletionSource<bool>(), tcs) == tcs)
                    return;
            }
        }
    }
}
