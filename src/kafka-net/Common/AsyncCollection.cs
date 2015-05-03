using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaNet.Common
{
    public class AsyncCollection<T>
    {
        private readonly object _lock = new object();
        private readonly AsyncManualResetEvent _dataAvailableEvent = new AsyncManualResetEvent();
	    private readonly IProducerConsumerCollection<T> _collection;
        private long _dataInBufferCount = 0;

	    public AsyncCollection()
	    {
			_collection = new ConcurrentBag<T>();
		}

	    public AsyncCollection(IProducerConsumerCollection<T> collection)
	    {
			_collection = collection;
		}

		public int Count
        {
            get { return _collection.Count + (int)Interlocked.Read(ref _dataInBufferCount); }
        }

        public bool IsCompleted { get; private set; }

        public void CompleteAdding()
        {
            IsCompleted = true;
        }

        public Task OnHasDataAvailable(CancellationToken token)
        {
            return _dataAvailableEvent.WaitAsync().WithCancellation(token);
        }

        public void Add(T data)
        {
            if (IsCompleted)
            {
                throw new ObjectDisposedException("AsyncCollection has been marked as complete.  No new documents can be added.");
            }

            _collection.TryAdd(data);
            TriggerDataAvailability();
        }

        public void AddRange(IEnumerable<T> data)
        {
            foreach (var item in data)
            {
                Add(item);
            }
        }
        
        public T Pop()
        {
            T data;
            return TryTake(out data) ? data : default(T);
        }

        public async Task<List<T>> TakeAsync(int count, TimeSpan timeout, CancellationToken token)
        {
            var batch = new List<T>(count);
            var timeoutTask = Task.Delay(timeout, token);

            try
            {
                do
                {
                    T data;
                    while (TryTake(out data))
                    {
                        batch.Add(data);
                        Interlocked.Increment(ref _dataInBufferCount);
                        if (--count <= 0) return batch;
                    }
                } while (await Task.WhenAny(_dataAvailableEvent.WaitAsync(), timeoutTask) != timeoutTask);

                return batch;
            }
            catch
            {
                return batch;
            }
            finally
            {
                Interlocked.Add(ref _dataInBufferCount, -1 * batch.Count);
            }
        }

        public void DrainAndApply(Action<T> appliedFunc)
        {
            T data;
            while (_collection.TryTake(out data))
            {
                appliedFunc(data);
            }

            TriggerDataAvailability();
        }

        public IEnumerable<T> Drain()
        {
            T data;
            while (_collection.TryTake(out data))
            {
                yield return data;
            }

            TriggerDataAvailability();
        }

        public bool TryTake(out T data)
        {
            try
            {
                return _collection.TryTake(out data);
            }
            finally
            {
                if (_collection.Count == 0) TriggerDataAvailability();
            }
        }

        private void TriggerDataAvailability()
        {
            lock (_lock)
            {
                if (_collection.Count == 0)
                {
                    _dataAvailableEvent.Reset();
                }
                else
                {
                    _dataAvailableEvent.Set();
                }
            }
        }
    }
}
