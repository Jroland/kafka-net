using System;
using System.Collections.Generic;
using KafkaNet.Common;

namespace KafkaNet.Model
{
    public class Topic
    {
        public Int16 ErrorCode { get; set; }
        public string Name { get; set; }
        public List<Partition> Partitions { get; set; }

        public static Topic FromStream(ReadByteStream stream)
        {
            var topic = new Topic
                {
                    ErrorCode = stream.ReadInt16(),
                    Name = stream.ReadInt16String(),
                    Partitions = new List<Partition>()
                };

            var numPartitions = stream.ReadInt();
            for (int i = 0; i < numPartitions; i++)
            {
                topic.Partitions.Add(Partition.FromStream(stream));
            }

            return topic;
        }
    }

    public class Partition
    {
        public Int16 ErrorCode { get; set; }
        public int PartitionId { get; set; }
        public int LeaderId { get; set; }
        public List<int> Replicas { get; set; }
        public List<int> Isrs { get; set; }

        public static Partition FromStream(ReadByteStream stream)
        {
            var partition = new Partition {
                ErrorCode = stream.ReadInt16(),
                PartitionId = stream.ReadInt(),
                LeaderId = stream.ReadInt(),
                Replicas = new List<int>(),
                Isrs = new List<int>()
            };

            var numReplicas = stream.ReadInt();
            for (int i = 0; i < numReplicas; i++)
            {
                partition.Replicas.Add(stream.ReadInt());
            }

            var numIsr = stream.ReadInt();
            for (int i = 0; i < numIsr; i++)
            {
                partition.Isrs.Add(stream.ReadInt());
            }

            return partition;
        }
    }

}
