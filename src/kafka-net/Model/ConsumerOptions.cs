﻿using System;
using System.Collections.Generic;

namespace KafkaNet.Model
{
    public class ConsumerOptions
    {
        private const int DefaultMaxConsumerBufferSize = 50;
        private const int DefaultBackoffIntervalMS = 100;

        /// <summary>
        /// The topic to consume messages from.
        /// </summary>
        public string Topic { get; set; }
        /// <summary>
        /// Whitelist of partitions to consume from.  Empty list indicates all partitions.
        /// </summary>
        public List<int> PartitionWhitelist { get; set; }
        /// <summary>
        /// The time in milliseconds between queries to look for any new partitions being created.
        /// </summary>
        public int TopicPartitionQueryTimeMs { get; set; }
        /// <summary>
        /// The size of the internal buffer queue which stores messages from Kafka.
        /// </summary>
        public int ConsumerBufferSize { get; set; }
        /// <summary>
        /// The interval (ms) for the consumer to sleep before try fetch next message if previous fetch received no message. 
        /// </summary>
        public int BackoffInterval { get; set; }

        public ConsumerOptions(string topic)
        {
            Topic = topic;
            PartitionWhitelist = new List<int>();
            TopicPartitionQueryTimeMs = (int)TimeSpan.FromMinutes(15).TotalMilliseconds;
            ConsumerBufferSize = DefaultMaxConsumerBufferSize;
            BackoffInterval = DefaultBackoffIntervalMS;
        }
    }
}