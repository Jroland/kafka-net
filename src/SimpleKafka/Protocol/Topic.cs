using System;
using System.Collections.Generic;
using SimpleKafka.Common;

namespace SimpleKafka.Protocol
{
    public class Topic
    {
        public Int16 ErrorCode { get; set; }
        public string Name { get; set; }
        public List<Partition> Partitions { get; set; }

        internal static Topic Decode(ref BigEndianDecoder decoder)
        {
            var topic = new Topic
            {
                ErrorCode = decoder.ReadInt16(),
                Name = decoder.ReadInt16String(),
            };

            var numPartitions = decoder.ReadInt32();
            var partitions = new List<Partition>(numPartitions);
            for (int i = 0; i < numPartitions; i++)
            {
                partitions.Add(Partition.Decode(ref decoder));
            }
            topic.Partitions = partitions;

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

        public static Partition Decode(ref BigEndianDecoder decoder)
        {
            var partition = new Partition {
                ErrorCode = decoder.ReadInt16(),
                PartitionId = decoder.ReadInt32(),
                LeaderId = decoder.ReadInt32(),
            };

            var numReplicas = decoder.ReadInt32();
            var replicas = new List<int>(numReplicas);
            for (int i = 0; i < numReplicas; i++)
            {
                replicas.Add(decoder.ReadInt32());
            }
            partition.Replicas = replicas;

            var numIsr = decoder.ReadInt32();
            var isrs = new List<int>(numIsr);
            for (int i = 0; i < numIsr; i++)
            {
                isrs.Add(decoder.ReadInt32());
            }
            partition.Isrs = isrs;

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
