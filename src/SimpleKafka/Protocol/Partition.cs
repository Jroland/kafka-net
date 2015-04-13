using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleKafka.Protocol
{
    public class Partition
    {
        /// <summary>
        /// Error code. 0 indicates no error occured.
        /// </summary>
        public readonly ErrorResponseCode ErrorCode;
        /// <summary>
        /// The Id of the partition that this metadata describes.
        /// </summary>
        public readonly int PartitionId;
        /// <summary>
        /// The node id for the kafka broker currently acting as leader for this partition. If no leader exists because we are in the middle of a leader election this id will be -1.
        /// </summary>
        public readonly int LeaderId;
        /// <summary>
        /// The set of alive nodes that currently acts as slaves for the leader for this partition.
        /// </summary>
        public readonly int[] Replicas;
        /// <summary>
        /// The set subset of the replicas that are "caught up" to the leader
        /// </summary>
        public readonly int[] Isrs;

        private Partition(ErrorResponseCode errorCode, int partitionId, int leaderId, int[] replicas, int[] isrs)
        {
            this.ErrorCode = errorCode;
            this.PartitionId = partitionId;
            this.LeaderId = leaderId;
            this.Replicas = replicas;
            this.Isrs = isrs;
        }

        internal static Partition Decode(KafkaDecoder decoder)
        {
            var errorCode = decoder.ReadErrorResponseCode();
            var partitionId = decoder.ReadInt32();
            var leaderId = decoder.ReadInt32();

            var numReplicas = decoder.ReadInt32();
            var replicas = new int[numReplicas];
            for (int i = 0; i < numReplicas; i++)
            {
                replicas[i] = decoder.ReadInt32();
            }

            var numIsr = decoder.ReadInt32();
            var isrs = new int[numIsr];
            for (int i = 0; i < numIsr; i++)
            {
                isrs[i] = decoder.ReadInt32();
            }
            var partition = new Partition(errorCode, partitionId, leaderId, replicas, isrs);

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
            return Equals((Partition)obj);
        }
    }
}
