using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;

namespace KafkaNet.Common
{
    public class ConcurrentCircularBuffer<T> : IEnumerable<T>
    {
        private readonly int _maxSize;
        private long _count = 0;
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
            if (Interlocked.Increment(ref _head) > (_maxSize - 1))
                Interlocked.Exchange(ref _head, 0);

            _values[_head] = obj;

            Interlocked.Exchange(ref _count,
                Math.Min(Interlocked.Increment(ref _count), _maxSize));

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
