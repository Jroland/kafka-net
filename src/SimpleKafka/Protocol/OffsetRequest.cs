using System;
using System.Collections.Generic;
using System.Linq;
using SimpleKafka.Common;

namespace SimpleKafka.Protocol
{
    /// <summary>
    /// A funky Protocol for requesting the starting offset of each segment for the requested partition 
    /// </summary>
    public class OffsetRequest : BaseRequest, IKafkaRequest<List<OffsetResponse>>
    {
        public ApiKeyRequestType ApiKey { get { return ApiKeyRequestType.Offset; } }
        public List<Offset> Offsets { get; set; }

        public void Encode(ref BigEndianEncoder encoder)
        {
            EncodeOffsetRequest(this, ref encoder);
        }

        public List<OffsetResponse> Decode(ref BigEndianDecoder decoder)
        {
            return DecodeOffsetResponse(ref decoder);
        }

        private static void EncodeOffsetRequest(OffsetRequest request, ref BigEndianEncoder encoder)
        {
            if (request.Offsets == null) request.Offsets = new List<Offset>();
            EncodeHeader(request, ref encoder);
            encoder.Write(ReplicaId);

            var topicGroups = request.Offsets.GroupBy(x => x.Topic).ToList();
            encoder.Write(topicGroups.Count);

            foreach (var topicGroup in topicGroups)
            {
                var partitions = topicGroup.GroupBy(x => x.PartitionId).ToList();
                encoder.Write(topicGroup.Key, StringPrefixEncoding.Int16);
                encoder.Write(partitions.Count);

                foreach (var partition in partitions)
                {
                    foreach (var offset in partition)
                    {
                        encoder.Write(partition.Key);
                        encoder.Write(offset.Time);
                        encoder.Write(offset.MaxOffsets);
                    }
                }
            }

        }


        private static List<OffsetResponse> DecodeOffsetResponse(ref BigEndianDecoder decoder)
        {
            var correlationId = decoder.ReadInt32();

            var responses = new List<OffsetResponse>();
            var topicCount = decoder.ReadInt32();
            for (int i = 0; i < topicCount; i++)
            {
                var topic = decoder.ReadInt16String();

                var partitionCount = decoder.ReadInt32();
                for (int j = 0; j < partitionCount; j++)
                {
                    var response = new OffsetResponse()
                    {
                        Topic = topic,
                        PartitionId = decoder.ReadInt32(),
                        Error = decoder.ReadInt16(),
                        Offsets = new List<long>()
                    };
                    var offsetCount = decoder.ReadInt32();
                    for (int k = 0; k < offsetCount; k++)
                    {
                        response.Offsets.Add(decoder.ReadInt64());
                    }

                    responses.Add(response);
                }
            }
            return responses;
        }
        
    }

    public class Offset
    {
        public Offset()
        {
            Time = -1;
            MaxOffsets = 1;
        }
        public string Topic { get; set; }
        public int PartitionId { get; set; }
        /// <summary>
        /// Used to ask for all messages before a certain time (ms). There are two special values. 
        /// Specify -1 to receive the latest offsets and -2 to receive the earliest available offset. 
        /// Note that because offsets are pulled in descending order, asking for the earliest offset will always return you a single element.
        /// </summary>
        public long Time { get; set; }
        public int MaxOffsets { get; set; }
    }

    public class OffsetResponse
    {
        public string Topic { get; set; }
        public int PartitionId { get; set; }
        public Int16 Error { get; set; }
        public List<long> Offsets { get; set; }
    }

    public class OffsetPosition
    {
        public OffsetPosition() { }
        public OffsetPosition(int partitionId, long offset)
        {
            PartitionId = partitionId;
            Offset = offset;
        }
        public int PartitionId { get; set; }
        public long Offset { get; set; }

        public override string ToString()
        {
            return string.Format("PartitionId:{0}, Offset:{1}", PartitionId, Offset);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((OffsetPosition)obj);
        }

        protected bool Equals(OffsetPosition other)
        {
            return PartitionId == other.PartitionId && Offset == other.Offset;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (PartitionId * 397) ^ Offset.GetHashCode();
            }
        }
    }
}