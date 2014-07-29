using System;
using System.Collections.Generic;

namespace KafkaNet.Configuration
{
    public class DefaultContainer : IContainer
    {
        private readonly Dictionary<Type, object> _factories = new Dictionary<Type, object>();
        private readonly Dictionary<Type, object> _instances = new Dictionary<Type, object>(); 

        public T Resolve<T>() where T: class
        {
            object instance;
            if (_instances.TryGetValue(typeof (T), out instance))
                return (T) instance;
            object factory;
            if (_factories.TryGetValue(typeof (T), out factory))
            {
                var newInstance = ((Func<IServiceProvider, T>) factory)(this);
                _instances.Add(typeof(T), newInstance);
                return newInstance;
            }
            throw new ServiceNotFound(string.Format("No service of type {0} has been registered", typeof(T).Name));
        }

        public IServiceRegistrator Register<T>(Func<IServiceProvider, T> factory) where T : class
        {
            if(_factories.ContainsKey(typeof(T)))
                return this;
            _factories.Add(typeof(T), factory);
            return this;
        }
    }
}