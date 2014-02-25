using System.Collections.Generic;
using KafkaNet.Model;

namespace KafkaNet
{
    public interface IPartitionSelector
    {
        Partition Select(Topic topic, string key);
    }
}