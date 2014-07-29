using KafkaNet.Protocol;

namespace KafkaNet
{
    public interface IPartitionSelector
    {
        Partition Select(Topic topic, string key);
    }
}