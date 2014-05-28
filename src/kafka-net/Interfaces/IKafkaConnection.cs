using System;

namespace KafkaNet
{
    public interface IKafkaConnection : IDisposable
    {
        Uri KafkaUri { get; }
        bool ReadPolling { get; }
        System.Threading.Tasks.Task<System.Collections.Generic.List<T>> SendAsync<T>(IKafkaRequest<T> request);
    }
}
