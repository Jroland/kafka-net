using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using KafkaNet.Common;
using KafkaNet.Model;

namespace KafkaNet
{
    public class DefaultPartitionSelector : IPartitionSelector
    {
        private readonly ConcurrentDictionary<string, Partition> _roundRobinTracker = new ConcurrentDictionary<string, Partition>();
        public Partition Select(string topic, string key, List<Partition> partitions)
        {
            if (partitions == null) throw new ArgumentNullException("partitions");
            if (partitions.Count <= 0) throw new ApplicationException(string.Format("Topic ({0}) has no partitions.", topic));

            //use round robing
            if (key == null)
            {
                return _roundRobinTracker.AddOrUpdate(topic, x => partitions.First(), (s, i) =>
                    {
                        var index = partitions.FindIndex(0, p => p.Equals(i));
                        if (index == -1) return partitions.First();
                        if (++index >= partitions.Count) return partitions.First();
                        return partitions[index];
                    });
            }
            
            //use key hash
            var partitionId = key.GetHashCode() % partitions.Count;
            var partition = partitions.FirstOrDefault(x => x.PartitionId == partitionId);

            if (partition == null)
                throw new InvalidPartitionIdSelectedException(string.Format("Hash function return partition id: {0}, but the available partitions are:{1}",
                                                                            partitionId, string.Join(",", partitions.Select(x => x.PartitionId))));

            return partition;
        }
    }
}