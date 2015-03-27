using System;
using System.Collections.Generic;
using KafkaNet.Protocol;

namespace KafkaNet.Model
{
    public class ConsumerOptions
    {
        private const int DefaultMaxConsumerBufferSize = 50;
        private const int DefaultBackoffIntervalMS = 1000;
        private const double DefaulFetchBufferMultiplier = 1.5;

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
        /// <summary>
        /// The max wait time is the maximum amount of time in milliseconds to block waiting if insufficient data is available at the time the request is issued.
        /// </summary>
        public TimeSpan MaxWaitTimeForMinimumBytes { get; set; }
        /// <summary>
        /// This is the minimum number of bytes of messages that must be available to give a response. If the client sets this to 0 the server will always respond immediately, 
        /// however if there is no new data since their last request they will just get back empty message sets. If this is set to 1, the server will respond as soon as at least 
        /// one partition has at least 1 byte of data or the specified timeout occurs. By setting higher values in combination with the timeout the consumer can tune for throughput 
        /// and trade a little additional latency for reading only large chunks of data (e.g. setting MaxWaitTime to 100 ms and setting MinBytes to 64k would allow the server to wait 
        /// up to 100ms to try to accumulate 64k of data before responding).
        /// </summary>
        public int MinimumBytes { get; set; }

        /// <summary>
        /// In the event of a buffer under run, this multiplier will allow padding the new buffer size.
        /// </summary>
        public double FetchBufferMultiplier { get; set; }

        public ConsumerOptions(string topic, IBrokerRouter router)
        {
            Topic = topic;
            Router = router;
            PartitionWhitelist = new List<int>();
            Log = new DefaultTraceLog();
            TopicPartitionQueryTimeMs = (int)TimeSpan.FromMinutes(15).TotalMilliseconds;
            ConsumerBufferSize = DefaultMaxConsumerBufferSize;
            BackoffInterval = TimeSpan.FromMilliseconds(DefaultBackoffIntervalMS);
            FetchBufferMultiplier = DefaulFetchBufferMultiplier;
            MaxWaitTimeForMinimumBytes = TimeSpan.FromMilliseconds(FetchRequest.DefaultMaxBlockingWaitTime);
            MinimumBytes = FetchRequest.DefaultMinBlockingByteBufferSize;
        }
    }
}