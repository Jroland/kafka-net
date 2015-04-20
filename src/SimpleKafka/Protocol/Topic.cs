using System;
using System.Collections.Generic;
using SimpleKafka.Common;

namespace SimpleKafka.Protocol
{
    public class Topic
    {
        public readonly ErrorResponseCode ErrorCode;
        public readonly string Name;
        public readonly Partition[] Partitions;

        private Topic(ErrorResponseCode errorCode, string name, Partition[] partitions)
        {
            this.ErrorCode = errorCode;
            this.Name = name;
            this.Partitions = partitions;
        }

        internal static Topic Decode(KafkaDecoder decoder)
        {
            var errorCode = decoder.ReadErrorResponseCode();
            var name = decoder.ReadString();

            var numPartitions = decoder.ReadInt32();
            var partitions = new Partition[numPartitions];
            for (int i = 0; i < numPartitions; i++)
            {
                partitions[i] = Partition.Decode(decoder);
            }
            var topic = new Topic(errorCode, name, partitions);

            return topic;
        }
    }


}
