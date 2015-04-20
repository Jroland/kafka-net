using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleKafka
{
    class FixedPartitioner<TPartitionKey> : IKafkaMessagePartitioner<TPartitionKey>
    {
        private readonly int partitionNumber;
        public FixedPartitioner(int partitionNumber)
        {
            this.partitionNumber = partitionNumber;
        }

        public int CalculatePartition(TPartitionKey key, int numberOfPartitions)
        {
            if (partitionNumber >= numberOfPartitions)
            {
                throw new InvalidOperationException(
                    string.Format(
                    "Fixed partition number ({0}) is more than the number of partitions ({1})",
                    partitionNumber, numberOfPartitions));

            }
            return partitionNumber;
        }

    }
}
