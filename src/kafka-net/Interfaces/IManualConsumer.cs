
using System.Collections.Generic;
using System.Threading.Tasks;
using KafkaNet.Protocol;

namespace KafkaNet.Interfaces
{
    public interface IManualConsumer
    {
        Task UpdateOrCreateOffset(string consumerGroup, long offset);

        Task<long> FetchLastOffset();

        Task<long> FetchOffset(string consumerGroup);

        Task<IEnumerable<Message>> FetchMessages(int maxCount, long offset);
    }
}
