using System;

namespace KafkaNet.Configuration
{
    public interface IServiceRegistrator
    {
        void Register<T>(T implementation) where T : class;
        void Register<T>(Func<IServiceProvider, T> factory) where T : class;
    }
}