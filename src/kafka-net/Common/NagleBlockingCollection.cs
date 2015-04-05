using System;
using System.Collections.Generic;
using System.Linq;
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
        private readonly int _boundedCapacity;
        private readonly AsyncCollection<T> _collection = new AsyncCollection<T>();
        private readonly SemaphoreSlim _boundedCapacitySemaphore;

        public NagleBlockingCollection(int boundedCapacity)
        {
            _boundedCapacity = boundedCapacity;
            _boundedCapacitySemaphore = new SemaphoreSlim(boundedCapacity, boundedCapacity);
        }

        public bool IsCompleted { get; private set; }

        public int Count { get { return _boundedCapacity - _boundedCapacitySemaphore.CurrentCount; } }

        public void CompleteAdding()
        {
            IsCompleted = true;
        }

        public Task AddRangeAsync(IEnumerable<T> data, CancellationToken token)
        {
            return Task.WhenAll(data.Select(x => AddAsync(x, token)));
        }

        public async Task AddAsync(T data, CancellationToken token)
        {
            if (IsCompleted)
            {
                throw new ObjectDisposedException("NagleBlockingCollection is currently being disposed.  Cannot add documents.");
            }

            await _boundedCapacitySemaphore.WaitAsync(token).ConfigureAwait(false);
            _collection.Add(data);
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
            List<T> batch = null;
            try
            {
                await _collection.OnDataAvailable(cancellationToken).ConfigureAwait(false);

                batch = await _collection.TakeAsync(batchSize, timeout, cancellationToken).ConfigureAwait(false);
               
                return batch;
            }
            catch
            {
                return batch ?? new List<T>();  //just return what we have collected
            }
            finally
            {
                if (batch != null && batch.Count > 0) _boundedCapacitySemaphore.Release(batch.Count);
            }
        }

        /// <summary>
        /// Immediately drains and returns any remaining messages in the queue 
        /// </summary>
        /// <returns></returns>
        public IEnumerable<T> Drain()
        {
            return _collection.Drain();
        }

        public void Dispose()
        {
            using (_boundedCapacitySemaphore)
            {
                IsCompleted = true;
            }
        }
    }
}
