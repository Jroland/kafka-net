using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet.Protocol;

namespace KafkaNet
{
    public interface IBus : IDisposable
    {
        Task<List<ProduceResponse>> SendMessageAsync(string topic, IEnumerable<Message> messages, Int16 acks = 1, int timeoutMS = 1000, MessageCodec codec = MessageCodec.CodecNone);
        IEnumerable<Message> Consume(string topic, CancellationToken? token = null);
    }
}