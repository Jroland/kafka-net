using System;
using System.Collections.Generic;
using System.Threading;
using KafkaNet.Protocol;

namespace KafkaNet
{
    public interface IConsumer : IDisposable
    {
        IEnumerable<Message> Consume(CancellationToken? cancellationToken = null);
    }
}