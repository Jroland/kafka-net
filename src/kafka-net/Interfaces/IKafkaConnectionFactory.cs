using KafkaNet.Protocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace KafkaNet
{
    public interface IKafkaConnectionFactory
    {
        IKafkaConnection Create(Uri kafkaAddress, int responseTimeoutMs, IKafkaLog log);
    }
}
