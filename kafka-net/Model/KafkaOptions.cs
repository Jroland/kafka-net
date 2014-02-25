using System;
using System.Collections.Generic;
using System.Linq;

namespace KafkaNet.Model
{
    public class KafkaOptions
    {
        private const int DefaultResponseTimeout = 5000;

        public List<Uri> KafkaServerUri { get; set; }
        public IPartitionSelector PartitionSelector { get; set; }
        public int ResponseTimeoutMs { get; set; }
        public IKafkaLog Log { get; set; }

        public KafkaOptions(params Uri[] kafkaServerUri)
        {
            KafkaServerUri = kafkaServerUri.ToList();
            PartitionSelector = new DefaultPartitionSelector();
            ResponseTimeoutMs = DefaultResponseTimeout;
            Log = new DefaultTraceLog();
        }
    }
}
