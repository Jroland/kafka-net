using System.Collections.Generic;

namespace StatisticsTestLoader
{
    public interface IRecordSource
    {
        string Topic { get; }
        IEnumerable<KafkaRecord> Poll(long index);
        int QueueCount { get; }
    }
}