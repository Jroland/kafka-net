using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaNet
{
    public class DefaultKafkaConnectionFactory : IKafkaConnectionFactory
    {
        public IKafkaConnection Create(Uri kafkaAddress, int responseTimeoutMs, IKafkaLog log)
        {
            return new KafkaConnection(new KafkaTcpSocket(kafkaAddress), responseTimeoutMs, log);
        }
    }
}
