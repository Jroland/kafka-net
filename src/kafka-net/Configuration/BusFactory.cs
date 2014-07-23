using System;

namespace KafkaNet.Configuration
{
    public static class BusFactory
    {
        public static IBus Create(IKafkaOptions settings, Action<IServiceRegistrator> configure)
        {
            var container = new DefaultContainer();
            DefaultServiceRegistrator.Register(container);
            container.Register(settings);
            configure(container);
            return container.Resolve<IBus>();
        }
    }
}