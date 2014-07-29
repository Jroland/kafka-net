using Castle.MicroKernel.Registration;
using Castle.Windsor;
using KafkaNet.Configuration;

namespace KafkaNet.Windsor
{
    public class WindsorAdapter : IContainer
    {
        private readonly IWindsorContainer _container;

        public WindsorAdapter(IWindsorContainer container)
        {
            this._container = container;
        }

        public T Resolve<T>() where T : class
        {
            try
            {
                return _container.Resolve<T>();
            }
            catch (Castle.MicroKernel.ComponentNotFoundException exception)
            {
                throw new ServiceNotFound(string.Format("No service of type {0} has been registered", typeof(T).Name), exception);
            }
        }

        public IServiceRegistrator Register<T>(System.Func<IServiceProvider, T> factory) where T : class
        {
            if (!_container.Kernel.HasComponent(typeof(T)))
            {
                _container.Register(
                    Component.For<T>().UsingFactoryMethod(() => factory(this)).LifeStyle.Singleton
                    );
            }
            return this;

        }
    }
}