namespace KafkaNet
{
    public interface IConsumerFactory
    {
        IConsumer GetConsumer(string topic);
    }
}