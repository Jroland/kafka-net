using System;
using System.Collections.Generic;
using KafkaNet.Configuration;

namespace KafkaNet.Model
{
    public class KafkaOptions : IKafkaOptions
    {
        public KafkaOptions()
        {
            Timeout = TimeSpan.FromSeconds(30);
            QueueSize = int.MaxValue;
        }

        public IEnumerable<Uri> Hosts { get; set; }
        public TimeSpan Timeout { get; set; }
        public int QueueSize { get; set; }
    }
}
