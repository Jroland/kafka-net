using System;

namespace KafkaNet.Configuration
{
    public interface IServiceRegistrator
    {
        IServiceRegistrator Register<T>(Func<IServiceProvider, T> factory) where T : class;
    }
}