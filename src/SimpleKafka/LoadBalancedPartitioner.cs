using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleKafka
{
    public class LoadBalancedPartitioner<TPartitionKey> : IKafkaMessagePartitioner<TPartitionKey>
    {
        private int current;

        public int CalculatePartition(TPartitionKey partitionKey, int numberOfPartitions)
        {
            if (current >= numberOfPartitions)
            {
                current = 0;
            }
            var partition = current;
            current = (current + 1) % numberOfPartitions;
            return partition;
        }
    }
}
