using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
		private readonly SemaphoreSlim m_semaphore;
		private readonly Task<Releaser> m_releaser;

		public AsyncLock()
		{
			m_semaphore = new SemaphoreSlim(1, 1);
			m_releaser = Task.FromResult(new Releaser(this));
		}

		public Task<Releaser> LockAsync(CancellationToken canceller)
		{
			var wait = m_semaphore.WaitAsync(canceller);
			return wait.IsCompleted ?
				m_releaser :
				wait.ContinueWith((_, state) => new Releaser((AsyncLock)state),
					this, canceller,
					TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
		}

		public Task<Releaser> LockAsync()
		{
			var wait = m_semaphore.WaitAsync();
			return wait.IsCompleted ?
				m_releaser :
				wait.ContinueWith((_, state) => new Releaser((AsyncLock)state),
					this, CancellationToken.None,
					TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
		}

		public void Dispose()
		{
			this.Dispose(true);
		}

		protected void Dispose(bool disposing)
		{
			if (disposing)
			{
				using (m_semaphore) { }
				using (m_releaser) { }
			}
		}

		public struct Releaser : IDisposable
		{
			private readonly AsyncLock m_toRelease;

			internal Releaser(AsyncLock toRelease) { m_toRelease = toRelease; }

			public void Dispose()
			{
				if (m_toRelease != null)
				{
					m_toRelease.m_semaphore.Release();
				}
			}
		} 
	}
}
