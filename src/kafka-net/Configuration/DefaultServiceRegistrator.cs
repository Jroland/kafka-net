namespace KafkaNet.Configuration
{
    public static class DefaultServiceRegistrator
    {
        public static void Register(IServiceRegistrator registrator)
        {
            registrator.Register<IKafkaLog>(_ => new DefaultTraceLog())
                       .Register<IPartitionSelector>(_ => new DefaultPartitionSelector())
                       .Register<IKafkaConnectionFactory>(x => new DefaultKafkaConnectionFactory(x.Resolve<IKafkaLog>()))
                       .Register<IBrokerRouter>(x => new BrokerRouter(x.Resolve<IKafkaOptions>(), x.Resolve<IKafkaLog>(), x.Resolve<IPartitionSelector>(), x.Resolve<IKafkaConnectionFactory>()))
                       .Register<IProducer>(x => new Producer(x.Resolve<IBrokerRouter>(), x.Resolve<IKafkaOptions>()))
                       .Register<IConsumerFactory>(x => new ConsumerFactory(x.Resolve<IBrokerRouter>(), x.Resolve<IKafkaLog>()))
                       .Register<IBus>(x => new Bus(x.Resolve<IProducer>(), x.Resolve<IConsumerFactory>()));
        }
    }
}