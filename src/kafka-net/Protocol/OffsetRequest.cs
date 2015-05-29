using System;
using System.Collections.Generic;
using System.Linq;
using KafkaNet.Common;

namespace KafkaNet.Protocol
{
    /// <summary>
    /// A funky Protocol for requesting the starting offset of each segment for the requested partition 
    /// </summary>
    public class OffsetRequest : BaseRequest, IKafkaRequest<OffsetResponse>
    {
        public ApiKeyRequestType ApiKey { get { return ApiKeyRequestType.Offset; } }
        public List<Offset> Offsets { get; set; }

        public KafkaDataPayload Encode()
        {
            return EncodeOffsetRequest(this);
        }

        public IEnumerable<OffsetResponse> Decode(byte[] payload)
        {
            return DecodeOffsetResponse(payload);
        }

        private KafkaDataPayload EncodeOffsetRequest(OffsetRequest request)
        {
            if (request.Offsets == null) request.Offsets = new List<Offset>();
            using (var message = EncodeHeader(request))
            {
                var topicGroups = request.Offsets.GroupBy(x => x.Topic).ToList();
                message.Pack(ReplicaId)
                    .Pack(topicGroups.Count);

                foreach (var topicGroup in topicGroups)
                {
                    var partitions = topicGroup.GroupBy(x => x.PartitionId).ToList();
                    message.Pack(topicGroup.Key, StringPrefixEncoding.Int16)
                        .Pack(partitions.Count);

                    foreach (var partition in partitions)
                    {
                        foreach (var offset in partition)
                        {
                            message.Pack(partition.Key)
                                .Pack(offset.Time)
                                .Pack(offset.MaxOffsets);
                        }
                    }
                }

                return new KafkaDataPayload
                {
                    Buffer = message.Payload(),
                    CorrelationId = request.CorrelationId,
                    ApiKey = ApiKey
                };
            }
        }


        private IEnumerable<OffsetResponse> DecodeOffsetResponse(byte[] data)
        {
            using (var stream = new BigEndianBinaryReader(data))
            {
                var correlationId = stream.ReadInt32();

                var topicCount = stream.ReadInt32();
                for (int i = 0; i < topicCount; i++)
                {
                    var topic = stream.ReadInt16String();

                    var partitionCount = stream.ReadInt32();
                    for (int j = 0; j < partitionCount; j++)
                    {
                        var response = new OffsetResponse()
                        {
                            Topic = topic,
                            PartitionId = stream.ReadInt32(),
                            Error = stream.ReadInt16(),
                            Offsets = new List<long>()
                        };
                        var offsetCount = stream.ReadInt32();
                        for (int k = 0; k < offsetCount; k++)
                        {
                            response.Offsets.Add(stream.ReadInt64());
                        }

                        yield return response;
                    }
                }
            }
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