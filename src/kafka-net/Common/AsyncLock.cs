using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaNet.Common
{
	/// <summary>
	/// An asynchronous locking construct.
	/// </summary>
	/// <remarks>
	/// This is based on Stephen Toub's implementation here: http://blogs.msdn.com/b/pfxteam/archive/2012/02/12/10266988.aspx
	/// However, we're using SemaphoreSlim as the basis rather than AsyncSempahore, since in .NET 4.5 SemaphoreSlim implements the WaitAsync() method.
	/// </remarks>
	public class AsyncLock : IDisposable
	{
		private readonly SemaphoreSlim _semaphore;
		private readonly Task<Releaser> _releaser;

		public AsyncLock()
		{
			_semaphore = new SemaphoreSlim(1, 1);
			_releaser = Task.FromResult(new Releaser(this));
		}

        public bool IsLocked {
            get { return _semaphore.CurrentCount == 0; }
        }

		public Task<Releaser> LockAsync(CancellationToken canceller)
		{
			var wait = _semaphore.WaitAsync(canceller);
            
            if (wait.IsCanceled) throw new OperationCanceledException("Unable to aquire lock within timeout alloted.");

			return wait.IsCompleted ?
				_releaser :
				wait.ContinueWith((t, state) =>
				{
                    if (t.IsCanceled) throw new OperationCanceledException("Unable to aquire lock within timeout alloted.");
                    return new Releaser((AsyncLock) state);
				},  this, canceller, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
		}

		public Task<Releaser> LockAsync()
		{
			var wait = _semaphore.WaitAsync();
			return wait.IsCompleted ?
				_releaser :
				wait.ContinueWith((_, state) => new Releaser((AsyncLock)state),
					this, CancellationToken.None,
					TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
		}

		public void Dispose()
		{
			Dispose(true);
		}

		protected void Dispose(bool disposing)
		{
			if (disposing)
			{
				using (_semaphore) { }
				using (_releaser) { }
			}
		}

		public struct Releaser : IDisposable
		{
			private readonly AsyncLock _toRelease;

            internal Releaser(AsyncLock toRelease) { _toRelease = toRelease; }

			public void Dispose()
			{
                if (_toRelease != null)
				{
                    _toRelease._semaphore.Release();
				}
			}
		} 
	}
}
