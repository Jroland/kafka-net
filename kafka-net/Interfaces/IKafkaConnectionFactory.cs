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

    public class DefaultKafkaConnectionFactory: IKafkaConnectionFactory
    {
        public IKafkaConnection Create(Uri kafkaAddress, int responseTimeoutMs, IKafkaLog log)
        {
            return new KafkaConnection(kafkaAddress, responseTimeoutMs, log);
        }
    }
}
