namespace KafkaNet.Configuration
{
    public interface IServiceProvider
    {
        T Resolve<T>() where T : class;
    }
}