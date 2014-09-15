using KafkaNet.Model;
using System;

namespace KafkaNet
{
    public interface IKafkaConnectionFactory
    {
        IKafkaConnection Create(Uri kafkaAddress, int responseTimeoutMs, IKafkaLog log);
        KafkaEndpoint Resolve(Uri kafkaAddress, IKafkaLog log);
    }
}
