namespace KafkaNet.Configuration
{
    public static class DefaultServiceRegistrator
    {
        public static void Register(IServiceRegistrator registrator)
        {
            registrator.Register<IKafkaLog>(new DefaultTraceLog());
            registrator.Register<IPartitionSelector>(new DefaultPartitionSelector());
            registrator.Register<IKafkaConnectionFactory>(x => new DefaultKafkaConnectionFactory(x.Resolve<IKafkaLog>()));
            registrator.Register<IBrokerRouter>(x => new BrokerRouter(x.Resolve<IKafkaOptions>(), x.Resolve<IKafkaLog>(), x.Resolve<IPartitionSelector>(), x.Resolve<IKafkaConnectionFactory>()));
            registrator.Register<IProducer>(x => new Producer(x.Resolve<IBrokerRouter>(), x.Resolve<IKafkaOptions>()));
            registrator.Register<IConsumerFactory>(x => new ConsumerFactory(x.Resolve<IBrokerRouter>(), x.Resolve<IKafkaLog>()));
            registrator.Register<IBus>(x => new Bus(x.Resolve<IProducer>(), x.Resolve<IConsumerFactory>()));
        }
    }
}