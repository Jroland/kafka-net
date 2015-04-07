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
        private readonly ConcurrentBag<T> _bag = new ConcurrentBag<T>();

        public int Count
        {
            get { return _bag.Count; }
        }

        public bool IsCompleted { get; private set; }

        public void CompleteAdding()
        {
            IsCompleted = true;
        }

        public Task OnDataAvailable(CancellationToken token)
        {
            return _dataAvailableEvent.WaitAsync().WithCancellation(token);
        }

        public void Add(T data)
        {
            if (IsCompleted)
            {
                throw new ObjectDisposedException("AsyncCollection has been marked as complete.  No new documents can be added.");
            }

            _bag.Add(data);
            TriggerDataAvailability();
        }

        public void AddRange(IEnumerable<T> data)
        {
            foreach (var item in data)
            {
                Add(item);
            }
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
                        if (--count <= 0) return batch;
                    }
                } while (await Task.WhenAny(_dataAvailableEvent.WaitAsync(), timeoutTask) != timeoutTask);

                return batch;
            }
            catch
            {
                return batch;
            }
        }

        public bool TryTake(out T data)
        {
            try
            {
                return _bag.TryTake(out data);
            }
            finally
            {
                if (_bag.IsEmpty) TriggerDataAvailability();
            }
        }

        public IEnumerable<T> Drain()
        {
            T data;
            while (_bag.TryTake(out data))
            {
                yield return data;
            }

            TriggerDataAvailability();
        }

        private void TriggerDataAvailability()
        {
            lock (_lock)
            {
                if (_bag.IsEmpty)
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
