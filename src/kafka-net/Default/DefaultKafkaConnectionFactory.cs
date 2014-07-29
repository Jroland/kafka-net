using System;

namespace KafkaNet
{
    public class DefaultKafkaConnectionFactory : IKafkaConnectionFactory
    {
        private readonly IKafkaLog kafkaLog;
        
        public DefaultKafkaConnectionFactory(IKafkaLog kafkaLog)
        {
            this.kafkaLog = kafkaLog;
        }

        public IKafkaConnection Create(Uri kafkaAddress, TimeSpan timeout)
        {
            return new KafkaConnection(new KafkaTcpSocket(kafkaLog, kafkaAddress), (int)timeout.TotalMilliseconds, kafkaLog);
        }
    }
}
