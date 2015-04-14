using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleKafka
{
    public class ReceivedKafkaMessage<TKey,TValue>
    {
        private readonly int partition;
        public int Partition {  get { return partition;  } }
        private readonly long offset;
        public long Offset {  get { return offset; } }
        private readonly TKey key;
        public TKey Key { get { return key; } }
        private readonly string topic;
        public string Topic { get { return topic; } }
        private readonly TValue value;
        public TValue Value { get { return value; } }

        public ReceivedKafkaMessage(string topic, TKey key, TValue value, int partition, long offset)
        {
            this.partition = partition;
            this.offset = offset;
            this.key = key;
            this.topic = topic;
            this.value = value;
        }

    }
}
