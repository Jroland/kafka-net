using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleKafka
{
    public class LoadBalancedPartitioner<TKey, TValue> : IKafkaMessagePartitioner<TKey, TValue>
    {
        private readonly int numberOfPartitions;
        public LoadBalancedPartitioner(int numberOfPartitions)
        {
            this.numberOfPartitions = numberOfPartitions;
        }

        private int current;

        public int CalculatePartition(KafkaMessage<TKey, TValue> message)
        {
            var partition = current;
            current = (current + 1) % numberOfPartitions;
            return partition;
        }
    }
}
