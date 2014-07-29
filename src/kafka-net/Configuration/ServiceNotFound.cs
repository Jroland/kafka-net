using System;

namespace KafkaNet.Configuration
{
    public class ServiceNotFound : Exception
    {
        public ServiceNotFound(string message, Exception exception) : base(message, exception)
        {
        }

        public ServiceNotFound(string message)
            : base(message)
        {
        }
    }
}