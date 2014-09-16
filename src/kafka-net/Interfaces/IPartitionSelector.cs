using System.Collections.Generic;
using KafkaNet.Model;
using KafkaNet.Protocol;

namespace KafkaNet
{
    public interface IPartitionSelector
    {
        Partition Select(Topic topic, byte[] key);
    }
}