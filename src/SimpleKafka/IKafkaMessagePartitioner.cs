using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleKafka
{
    public interface IKafkaMessagePartitioner<TKey,TValue>
    {
        int CalculatePartition(KafkaMessage<TKey, TValue> message);
    }
}
