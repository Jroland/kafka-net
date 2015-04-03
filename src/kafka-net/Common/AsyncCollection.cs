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

        public Task OnDataAvailable(CancellationToken token)
        {
            return _dataAvailableEvent.WaitAsync().WithCancellation(token);
        }

        public async Task<bool> OnDataAvailable(TimeSpan timeout, CancellationToken token)
        {
            var waitTask = _dataAvailableEvent.WaitAsync();
            return await Task.WhenAny(waitTask, Task.Delay(timeout, token)) == waitTask;
        }

        public void Add(T data)
        {
            _bag.Add(data);
            TriggerDataAvailability();
        }

        public void AddRange(IEnumerable<T> data)
        {
            foreach (var item in data)
            {
                _bag.Add(item);
            }
            TriggerDataAvailability();
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
