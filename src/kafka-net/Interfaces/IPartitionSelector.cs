using KafkaNet.Protocol;

namespace KafkaNet
{
    public interface IPartitionSelector
    {
        /// <summary>
        /// Select the appropriate partition post a message based on topic and key data.
        /// </summary>
        /// <param name="topic">The topic at which the message will be sent.</param>
        /// <param name="key">The data used to consistently route a message to a particular partition.  Value can be null.</param>
        /// <returns>The partition to send the message to.</returns>
        Partition Select(Topic topic, byte[] key);
    }
}