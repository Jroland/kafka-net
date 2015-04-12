using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleKafka
{
    public class ReceivedKafkaMessage<TKey,TValue> : KafkaMessage<TKey,TValue>
    {
        private readonly int partition;
        public int Partition {  get { return partition;  } }
        private readonly long offset;
        public long Offset {  get { return offset; } }

        public ReceivedKafkaMessage(string topic, TKey key, TValue value, int partition, long offset) : base(topic, key, value)
        {
            this.partition = partition;
            this.offset = offset;
        }

    }
}
