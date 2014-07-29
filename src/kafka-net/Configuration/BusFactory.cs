using System;

namespace KafkaNet.Configuration
{
    public static class BusFactory
    {
        public static IBus Create(IKafkaOptions options, Action<TinyIoC.TinyIoCContainer> configure)
        {
            var container = TinyIoC.TinyIoCContainer.Current;

            container.AutoRegister();

            container.Register<IKafkaOptions>(options);
            container.Register<IKafkaLog, DefaultTraceLog>().AsSingleton();
            container.Register<IPartitionSelector, DefaultPartitionSelector>().AsSingleton();
            container.Register<IKafkaConnectionFactory, DefaultKafkaConnectionFactory>().AsSingleton();
            container.Register<IBrokerRouter, BrokerRouter>().AsSingleton();

            configure(container);

            return container.Resolve<IBus>();
        }
    }
}