using System;
using KafkaNet.Model;

namespace KafkaNet
{
    public interface IKafkaConnection : IDisposable
    {
        KafkaEndpoint Endpoint { get; }
        bool ReadPolling { get; }
        System.Threading.Tasks.Task SendAsync(byte[] payload);
        System.Threading.Tasks.Task<System.Collections.Generic.List<T>> SendAsync<T>(IKafkaRequest<T> request);
    }
}
