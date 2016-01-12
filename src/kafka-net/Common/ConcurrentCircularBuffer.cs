using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace KafkaNet.Common
{
    public class ConcurrentCircularBuffer<T> : IEnumerable<T>
    {
        private readonly object _syncObject = new object();

        readonly ConcurrentQueue<T> _values = new ConcurrentQueue<T>();

        private readonly int _maxSize;


        public ConcurrentCircularBuffer(int max)
        {
            _maxSize = max;
        }

        public int MaxSize { get { return _maxSize; } }

        public long Count
        {
            get
            {
                return _values.Count;
            }
        }

        /// <summary>
        /// Add <see cref="obj"/> to buffer
        /// </summary>
        /// <remarks>If count exceeds <see cref="MaxSize"/> then oldest object (FIFO) will be evicted</remarks>
        /// <param name="obj"></param>
        public void Enqueue(T obj)
        {
            _values.Enqueue(obj);
            lock (_syncObject)
            {
                T overflow;
                while (_values.Count > _maxSize && _values.TryDequeue(out overflow)) { }
            }
        }

        /// <summary>
        /// returns a snapshot (moment-in-time) enumeration of the buffer
        /// </summary>
        /// <returns></returns>
        public IEnumerator<T> GetEnumerator()
        {
            return _values.GetEnumerator();
        }

        /// <summary>
        /// returns a snapshot (moment-in-time) enumeration of the buffer
        /// </summary>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
