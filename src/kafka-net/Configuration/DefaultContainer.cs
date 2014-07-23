using System;
using System.Collections.Generic;

namespace KafkaNet.Configuration
{
    public class DefaultContainer : IContainer
    {
        private readonly Dictionary<Type, object> _implementations = new Dictionary<Type, object>();
        private readonly Dictionary<Type, object> _factories = new Dictionary<Type, object>();

        public T Resolve<T>() where T: class
        {
            object implementation;
            if (_implementations.TryGetValue(typeof (T), out implementation))
                return (T) implementation;
            object factory;
            if (_factories.TryGetValue(typeof(T), out factory))
            {
                var serviceFactory = (Func<IServiceProvider, T>)factory;
                var serviceImplementation = serviceFactory(this);
                _implementations.Add(typeof (T), serviceImplementation);
                return serviceImplementation;
            }
            throw new ServiceImplementationNotFound();
        }

        public void Register<T>(T implementation) where T : class
        {
            _implementations.Add(typeof (T), implementation);
        }

        public void Register<T>(Func<IServiceProvider, T> factory) where T : class 
        {
            _factories.Add(typeof(T), factory);
        }
    }
}