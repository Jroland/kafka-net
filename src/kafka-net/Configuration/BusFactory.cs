using System;

namespace KafkaNet.Configuration
{
    public class BusFactory
    {
        private readonly IContainer container;

        public BusFactory(IContainer container)
        {
            this.container = container;
        }

        public BusFactory() : this(new DefaultContainer())
        {
        }

        public IBus Create(IKafkaOptions settings, Action<IServiceRegistrator> configure)
        {
            configure(container);
            container.Register(_ => settings);
            DefaultServiceRegistrator.Register(container);
            return container.Resolve<IBus>();
        }
    }
}