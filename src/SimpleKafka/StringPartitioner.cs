using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleKafka
{
    public class StringPartitioner : IKafkaMessagePartitioner<string>
    {
        public int CalculatePartition(string partitionKey, int numberOfPartitions)
        {
            return Math.Abs(partitionKey.GetHashCode() % numberOfPartitions);
        }
    }
}
