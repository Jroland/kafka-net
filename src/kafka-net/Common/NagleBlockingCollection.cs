using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaNet.Common
{
    /// <summary>
    /// Awaitable bounded capacity collection, which provides the ability to await
    /// inbound data without having to actively poll the data collection.  The initial
    /// call to TakeBatch will block until data arrives, then the method will try to
    /// take up to x items from the collection within a specified timespan and return
    /// all that it could grab up to the x item limit.
    /// 
    /// This collection attempts to implement the pattern similar to that of the Nagle 
    /// algorithm where an artificial delay is added to a send operation to see if a
    /// collection of send items can be batched together before sending.
    /// </summary>
    public class NagleBlockingCollection<T> : IDisposable
    {   
        private readonly BlockingCollection<T> _collection;
        private readonly SemaphoreSlim _dataAvailableSemaphore = new SemaphoreSlim(0);

        public NagleBlockingCollection(int boundedCapacity)
        {
            _collection = new BlockingCollection<T>(boundedCapacity);
        }

		public bool IsAddingCompleted { get { return _collection.IsAddingCompleted; } }
		public bool IsCompleted { get { return _collection.IsCompleted; } }

        public int Count { get { return _collection.Count; } }

        public void AddRange(IEnumerable<T> data)
        {
            foreach (var item in data)
            {
                Add(item);
            }
        }

        public void Add(T data)
        {
			if (_collection.IsAddingCompleted)
			{
				throw new ObjectDisposedException("NagleBlockingCollection is currently being disposed.  Cannot add documents.");
			}

            _collection.Add(data);
            _dataAvailableSemaphore.Release();
        }

        /// <summary>
        /// Block until data arrives and then attempt to take batchSize amount of data with timeout.
        /// </summary>
        /// <param name="batchSize">The amount of data to try and pull from the collection.</param>
        /// <param name="timeout">The maximum amount of time to wait until batchsize can be pulled from the collection.</param>
        /// <returns></returns>
        public Task<List<T>> TakeBatch(int batchSize, TimeSpan timeout)
        {
            return TakeBatch(batchSize, timeout, new CancellationToken());
        }

        /// <summary>
        /// Block until data arrives and then attempt to take batchSize amount of data with timeout.
        /// </summary>
        /// <param name="batchSize">The amount of data to try and pull from the collection.</param>
        /// <param name="timeout">The maximum amount of time to wait until batchsize can be pulled from the collection.</param>
        /// <param name="cancellationToken">Cancellation token to short cut the takebatch command.</param>
        /// <returns></returns>
        public async Task<List<T>> TakeBatch(int batchSize, TimeSpan timeout, CancellationToken cancellationToken)
        {
			var batch = new List<T>(Math.Max(_collection.Count, 10));
			try
			{
				await _dataAvailableSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

				do
				{
					batch.Add(_collection.Take(cancellationToken));
					if (--batchSize == 0) break;
				} while (await _dataAvailableSemaphore.WaitAsync(timeout, cancellationToken).ConfigureAwait(false) || _collection.Count > 0);
			}
			catch (OperationCanceledException)
			{
				//bury these so that we can return whatever messages we've already dequeued, instead of throwing them away
			}
            return batch;
        }

		/// <summary>
		/// Immediately drains and returns any remaining messages in the queue 
		/// </summary>
		/// <returns></returns>
		public List<T> Drain()
		{
			if (!_collection.IsAddingCompleted)
			{
				throw new InvalidOperationException("Should not try to drain the collection unless adding is complete");
			}

			T msg;
			var batch = new List<T>(_collection.Count);
			while (_collection.TryTake(out msg))
			{
				batch.Add(msg);
			}

			return batch;
		}

		public void CompleteAdding()
		{
			_collection.CompleteAdding();
		}

		public void Dispose()
		{
			using (_collection)
			using (_dataAvailableSemaphore)
			{ }
		}
    }
}
