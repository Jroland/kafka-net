using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleKafka
{
    public interface IKafkaMessagePartitioner<TPartitionKey>
    {
        int CalculatePartition(TPartitionKey partitionKey, int numberOfPartitions);
    }
}
