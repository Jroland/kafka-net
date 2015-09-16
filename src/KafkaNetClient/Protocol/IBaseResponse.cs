using System;

namespace KafkaNet.Protocol
{
    public interface IBaseResponse
    {
        Int16 Error { get; set; }
        string Topic { get; set; }
        int PartitionId { get; set; }
    }
}