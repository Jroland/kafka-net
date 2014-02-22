using System;
using System.Collections.Generic;

namespace KafkaNet.Model
{
    public class KafkaClientOptions
    {
        private const int DefaultResonseTimeout = 5000;

        public List<Uri> KafkaServerUri { get; set; }
        public IPartitionSelector PartitionSelector { get; set; }
        public int ResponseTimeoutMs { get; set; }

        public void KafkaConnection(List<Uri> kafkaServerUri)
        {
            KafkaServerUri = kafkaServerUri;
            PartitionSelector = new DefaultPartitionSelector();
            ResponseTimeoutMs = DefaultResonseTimeout;
        }
    }
}
