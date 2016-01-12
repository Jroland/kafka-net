using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;

namespace KafkaNet.Common
{
    public class ConcurrentCircularBuffer<T> : IEnumerable<T>
    {
        private readonly int _maxSize;
        private long _count;
        private int _head = -1;
        readonly T[] _values;

        public ConcurrentCircularBuffer(int max)
        {
            _maxSize = max;
            _values = new T[_maxSize];
        }

        public int MaxSize { get { return _maxSize; } }

        public long Count
        {
            get
            {
                return Interlocked.Read(ref _count);
            }
        }

        public ConcurrentCircularBuffer<T> Enqueue(T obj)
        {
            var head = Interlocked.Increment(ref _head);

            if (head > _maxSize - 1)
            {
                //this should exchange to correct index even if interlocked called twice from different threads
                Interlocked.Exchange(ref _head, head - _maxSize);
                head = head - _maxSize;
            }

            _values[head] = obj;

            if (_count != _maxSize) //once we hit max size we dont need to track count.
                Interlocked.Exchange(ref _count, Math.Min(Interlocked.Increment(ref _count), _maxSize));

            return this;
        }

        public IEnumerator<T> GetEnumerator()
        {
            for (int i = 0; i < Count; i++)
            {
                yield return _values[i];
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
