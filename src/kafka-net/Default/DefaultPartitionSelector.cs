using System;
using System.Collections.Concurrent;
using System.Linq;
using KafkaNet.Common;
using KafkaNet.Model;
using KafkaNet.Protocol;

namespace KafkaNet
{
    public class DefaultPartitionSelector : IPartitionSelector
    {
        private readonly ConcurrentDictionary<string, int> _roundRobinTracker = new ConcurrentDictionary<string, int>();

        public Partition Select(Topic topic, byte[] key)
        {
            if (topic == null) throw new ArgumentNullException("topic");
            if (topic.Partitions.Count <= 0) throw new ApplicationException(string.Format("Topic ({0}) has no partitions.", topic.Name));

            //use round robin
            var partitions = topic.Partitions;
            if (key == null)
            {
                //use round robin
                var paritionIndex = _roundRobinTracker.AddOrUpdate(topic.Name, p => 0, (s, i) =>
                    {
                        return ((i + 1) % partitions.Count);
                    });

                return partitions[paritionIndex];
            }

            //use key hash
            var partitionId = Crc32Provider.Compute(key) % partitions.Count;
            var partition = partitions.FirstOrDefault(x => x.PartitionId == partitionId);

            if (partition == null)
                throw new InvalidPartitionException(string.Format("Hash function return partition id: {0}, but the available partitions are:{1}",
                                                                            partitionId, string.Join(",", partitions.Select(x => x.PartitionId))));

            return partition;
        }
    }
}