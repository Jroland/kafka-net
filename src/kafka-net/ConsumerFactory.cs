using KafkaNet.Model;

namespace KafkaNet
{
    public class ConsumerFactory : IConsumerFactory
    {
        private readonly IBrokerRouter _brokerRouter;
        private readonly IKafkaLog _log;

        public ConsumerFactory(IBrokerRouter brokerRouter, IKafkaLog log)
        {
            _brokerRouter = brokerRouter;
            _log = log;
        }

        public IConsumer GetConsumer(string topic)
        {
            return new Consumer(_brokerRouter, _log, new ConsumerOptions(topic));
        }
    }
}