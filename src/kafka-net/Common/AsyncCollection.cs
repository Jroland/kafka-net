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
        private readonly ConcurrentQueue<T> _queue = new ConcurrentQueue<T>();
        private long _dataInBufferCount = 0;

        public int Count
        {
            get { return _queue.Count + (int)Interlocked.Read(ref _dataInBufferCount); }
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

            _queue.Enqueue(data);

            TriggerDataAvailability();
        }

        public void AddRange(IEnumerable<T> data)
        {
            if (IsCompleted)
            {
                throw new ObjectDisposedException("AsyncCollection has been marked as complete.  No new documents can be added.");
            }

            foreach (var item in data)
            {
                _queue.Enqueue(item);
            }

            TriggerDataAvailability();
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
                        if (--count <= 0 || timeoutTask.IsCompleted) return batch;
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
            while (_queue.TryDequeue(out data))
            {
                appliedFunc(data);
            }

            TriggerDataAvailability();
        }

        public IEnumerable<T> Drain()
        {
            T data;
            while (_queue.TryDequeue(out data))
            {
                yield return data;
            }

            TriggerDataAvailability();
        }

        public bool TryTake(out T data)
        {
            try
            {
                return _queue.TryDequeue(out data);
            }
            finally
            {
                if (_queue.IsEmpty) TriggerDataAvailability();
            }
        }

        private void TriggerDataAvailability()
        {
            if (_queue.IsEmpty && _dataAvailableEvent.IsOpen)
            {
                lock (_lock)
                {
                    if (_queue.IsEmpty && _dataAvailableEvent.IsOpen)
                    {
                        _dataAvailableEvent.Close();
                    }
                }
            }

            if (_queue.IsEmpty == false && _dataAvailableEvent.IsOpen == false)
            {
                lock (_lock)
                {
                    if (_queue.IsEmpty == false && _dataAvailableEvent.IsOpen == false)
                    {
                        _dataAvailableEvent.Open();
                    }
                }
            }

        }
    }
}
