using System;
using System.Collections.Generic;
using System.Linq;

namespace KafkaNet.Model
{
    public class KafkaClientOptions
    {
        private const int DefaultResonseTimeout = 5000;

        public List<Uri> KafkaServerUri { get; set; }
        public IPartitionSelector PartitionSelector { get; set; }
        public int ResponseTimeoutMs { get; set; }

        public KafkaClientOptions(params Uri[] kafkaServerUri)
        {
            KafkaServerUri = kafkaServerUri.ToList();
            PartitionSelector = new DefaultPartitionSelector();
            ResponseTimeoutMs = DefaultResonseTimeout;
        }
    }
}
