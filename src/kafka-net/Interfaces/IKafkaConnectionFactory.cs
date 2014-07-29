using System;

namespace KafkaNet
{
    public interface IKafkaConnectionFactory
    {
        IKafkaConnection Create(Uri kafkaAddress, TimeSpan timeout);
    }
}
