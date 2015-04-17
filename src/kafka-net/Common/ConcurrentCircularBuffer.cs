using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace KafkaNet.Common
{
    public class ConcurrentCircularBuffer<T> : IEnumerable<T>
    {
        private readonly int _maxSize;
        private readonly object _lock = new object();

        private int _count = 0;
        private int _head = -1;
        readonly T[] _values;

        public ConcurrentCircularBuffer(int max)
        {
            _maxSize = max;
            _values = new T[_maxSize];
        }

        public int MaxSize { get { return _maxSize; } }

        public int Count
        {
            get
            {
                lock (_lock)
                {
                    return _count;
                }
            }
        }

        public ConcurrentCircularBuffer<T> Enqueue(T obj)
        {
            lock (_lock)
            {
                if (Interlocked.Increment(ref _head) > (_maxSize - 1))
                    Interlocked.Exchange(ref _head, 0);

                _values[_head] = obj;

                _count = Math.Min(++_count, _maxSize);
            }

            return this;
        }

       

        public IEnumerator<T> GetEnumerator()
        {
            lock (_lock)
            {
                for (int i = 0; i < _count; i++)
                {
                    yield return _values[i];
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
