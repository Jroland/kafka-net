using System;
using System.Collections.Generic;

namespace KafkaNet.Model
{
    public class ConsumerOptions
    {
        private const int DefaultMaxConsumerBufferSize = 50;
        private const int DefaultBackoffIntervalMS = 1000;
		private const int DefaultMaxMsgSize = 4096 * 1024;	// default to be 4 MB for max message size.
        /// <summary>
        /// The topic to consume messages from.
        /// </summary>
        public string Topic { get; set; }
        /// <summary>
        /// Whitelist of partitions to consume from.  Empty list indicates all partitions.
        /// </summary>
        public List<int> PartitionWhitelist { get; set; }
        /// <summary>
        /// Log object to record operational messages.
        /// </summary>
        public IKafkaLog Log { get; set; }
        /// <summary>
        /// The broker router used to provide connection to each partition server.
        /// </summary>
        public IBrokerRouter Router { get; set; }
        /// <summary>
        /// The time in milliseconds between queries to look for any new partitions being created.
        /// </summary>
        public int TopicPartitionQueryTimeMs { get; set; }
        /// <summary>
        /// The size of the internal buffer queue which stores messages from Kafka.
        /// </summary>
        public int ConsumerBufferSize { get; set; }
        /// <summary>
        /// The interval for the consumer to sleep before try fetch next message if previous fetch received no message. 
        /// </summary>
        public TimeSpan BackoffInterval { get; set; }

        public int MaxMessageSize { get; set; }
        
        public ConsumerOptions(string topic, IBrokerRouter router)
        {
            Topic = topic;
            Router = router;
            PartitionWhitelist = new List<int>();
            Log = new DefaultTraceLog();
            TopicPartitionQueryTimeMs = (int)TimeSpan.FromMinutes(15).TotalMilliseconds;
            ConsumerBufferSize = DefaultMaxConsumerBufferSize;
            BackoffInterval = TimeSpan.FromMilliseconds(DefaultBackoffIntervalMS);
            MaxMessageSize = DefaultMaxMsgSize;
        }
    }
}