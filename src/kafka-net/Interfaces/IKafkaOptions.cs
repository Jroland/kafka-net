using System;
using System.Collections.Generic;

namespace KafkaNet
{
    public interface IKafkaOptions
    {
        ICollection<Uri> Hosts { get; }
        TimeSpan Timeout { get; }
        int QueueSize { get; }
    }
}