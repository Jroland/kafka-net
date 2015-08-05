
using System.Collections.Generic;
using System.Threading.Tasks;
using KafkaNet.Protocol;

namespace KafkaNet.Interfaces
{
    public interface IManualConsumer
    {
        Task UpdateOffset(string consumerGroup, long offset);

        Task<long> GetLastOffset();

        Task<long> GetOffset(string consumerGroup);

        Task<IEnumerable<Message>> GetMessages(int maxCount, long offset);
    }
}
