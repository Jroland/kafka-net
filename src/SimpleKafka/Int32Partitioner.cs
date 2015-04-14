using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleKafka
{
    public class Int32Partitioner : IKafkaMessagePartitioner<int>
    {
        public int CalculatePartition(int partitionKey, int numberOfPartitions)
        {
            return partitionKey % numberOfPartitions;
        }
    }
}
