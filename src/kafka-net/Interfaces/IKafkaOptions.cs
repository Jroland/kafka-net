using System;
using System.Collections.Generic;

namespace KafkaNet
{
    public interface IKafkaOptions
    {
        IEnumerable<Uri> Hosts { get; }
        TimeSpan Timeout { get; }
        int QueueSize { get; }
    }
}