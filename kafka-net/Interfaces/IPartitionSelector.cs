using System.Collections.Generic;
using KafkaNet.Model;

namespace KafkaNet
{
    public interface IPartitionSelector
    {
        Partition Select(string topic, string key, List<Partition> partitions);
    }
}