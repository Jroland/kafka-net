using System;

namespace KafkaNet.Model
{
    public class ConsumerOptions
    {
        public string Topic { get; set; }
        public IKafkaLog Log { get; set; }
        public BrokerRouter Router { get; set; }
        public int TopicMaxOffsetQueryTimeMs { get; set; }
        public int TopicPartitionQueryTimeMs { get; set; }
        public int ConsumerBufferSize { get; set; }

        public ConsumerOptions()
        {
            Log = new DefaultTraceLog();
            TopicMaxOffsetQueryTimeMs = 1000;
            TopicPartitionQueryTimeMs = (int)TimeSpan.FromMinutes(15).TotalMilliseconds;
            ConsumerBufferSize = 50;
        }
    }
}