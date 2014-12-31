using System;
using System.Collections.Generic;
using KafkaNet.Common;

namespace KafkaNet.Protocol
{
    public class Topic
    {
        public Int16 ErrorCode { get; set; }
        public string Name { get; set; }
        public List<Partition> Partitions { get; set; }

        public static Topic FromStream(BigEndianBinaryReader stream)
        {
            var topic = new Topic
                {
                    ErrorCode = stream.ReadInt16(),
                    Name = stream.ReadInt16String(),
                    Partitions = new List<Partition>()
                };

            var numPartitions = stream.ReadInt32();
            for (int i = 0; i < numPartitions; i++)
            {
                topic.Partitions.Add(Partition.FromStream(stream));
            }

            return topic;
        }
    }

    public class Partition
    {
        /// <summary>
        /// Error code. 0 indicates no error occured.
        /// </summary>
        public Int16 ErrorCode { get; set; }
        /// <summary>
        /// The Id of the partition that this metadata describes.
        /// </summary>
        public int PartitionId { get; set; }
        /// <summary>
        /// The node id for the kafka broker currently acting as leader for this partition. If no leader exists because we are in the middle of a leader election this id will be -1.
        /// </summary>
        public int LeaderId { get; set; }
        /// <summary>
        /// The set of alive nodes that currently acts as slaves for the leader for this partition.
        /// </summary>
        public List<int> Replicas { get; set; }
        /// <summary>
        /// The set subset of the replicas that are "caught up" to the leader
        /// </summary>
        public List<int> Isrs { get; set; }

        public static Partition FromStream(BigEndianBinaryReader stream)
        {
            var partition = new Partition {
                ErrorCode = stream.ReadInt16(),
                PartitionId = stream.ReadInt32(),
                LeaderId = stream.ReadInt32(),
                Replicas = new List<int>(),
                Isrs = new List<int>()
            };

            var numReplicas = stream.ReadInt32();
            for (int i = 0; i < numReplicas; i++)
            {
                partition.Replicas.Add(stream.ReadInt32());
            }

            var numIsr = stream.ReadInt32();
            for (int i = 0; i < numIsr; i++)
            {
                partition.Isrs.Add(stream.ReadInt32());
            }

            return partition;
        }

        protected bool Equals(Partition other)
        {
            return PartitionId == other.PartitionId;
        }

        public override int GetHashCode()
        {
            return PartitionId;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((Partition) obj);
        }
    }

}
