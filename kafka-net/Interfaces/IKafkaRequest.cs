using System.Collections.Generic;

namespace KafkaNet
{
    public interface IKafkaRequest<out T>
    {
        string ClientId { get; set; }
        int CorrelationId { get; set; }
        ApiKeyRequestType ApiKey { get; }
        byte[] Encode();
        IEnumerable<T> Decode(byte[] payload);
    }
}