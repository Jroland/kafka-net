using System;
using System.Collections.Generic;
using System.Linq;
using SimpleKafka.Common;

namespace SimpleKafka.Protocol
{
    /// <summary>
    /// A funky Protocol for requesting the starting offset of each segment for the requested partition 
    /// </summary>
    public class OffsetRequest : BaseRequest<List<OffsetResponse>>
    {
        public List<Offset> Offsets { get; set; }

        public OffsetRequest() : base(ApiKeyRequestType.Offset) { }

        internal override KafkaEncoder Encode(KafkaEncoder encoder)
        {
            return EncodeOffsetRequest(this, encoder);
        }

        internal override List<OffsetResponse> Decode(KafkaDecoder decoder)
        {
            return DecodeOffsetResponse(decoder);
        }

        private static KafkaEncoder EncodeOffsetRequest(OffsetRequest request, KafkaEncoder encoder)
        {
            request
                .EncodeHeader(encoder)
                .Write(ReplicaId);

            if (request.Offsets == null)
            {
                encoder.Write(0);
            }
            else if (request.Offsets.Count == 1)
            {
                // shortcut the single request
                var offset = request.Offsets[0];
                encoder
                    .Write(1)
                    .Write(offset.Topic)
                    .Write(1)
                    .Write(offset.PartitionId)
                    .Write(offset.Time)
                    .Write(offset.MaxOffsets);
            }
            else
            {
                // Full request
                var topicGroups = new Dictionary<string, List<Offset>>();
                foreach (var offset in request.Offsets)
                {
                    var offsets = topicGroups.GetOrCreate(offset.Topic, () => new List<Offset>(request.Offsets.Count));
                    offsets.Add(offset);
                }

                encoder.Write(topicGroups.Count);
                foreach (var kvp in topicGroups)
                {
                    var topic = kvp.Key;
                    var offsets = kvp.Value;

                    encoder
                        .Write(topic)
                        .Write(offsets.Count);

                    foreach (var offset in offsets)
                    {
                        encoder
                            .Write(offset.PartitionId)
                            .Write(offset.Time)
                            .Write(offset.MaxOffsets);
                    }
                }
            }
            return encoder;
        }



        private static List<OffsetResponse> DecodeOffsetResponse(KafkaDecoder decoder)
        {
            var correlationId = decoder.ReadInt32();

            var responses = new List<OffsetResponse>();
            var topicCount = decoder.ReadInt32();
            for (int i = 0; i < topicCount; i++)
            {
                var topic = decoder.ReadString();

                var partitionCount = decoder.ReadInt32();
                for (int j = 0; j < partitionCount; j++)
                {
                    var response = OffsetResponse.Decode(decoder, topic);

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
        public readonly string Topic;
        public readonly int PartitionId;
        public readonly ErrorResponseCode Error;
        public readonly long[] Offsets;

        private OffsetResponse(string topic, int partitionId, ErrorResponseCode error, long[] offsets)
        {
            this.Topic = topic;
            this.PartitionId = partitionId;
            this.Error = error;
            this.Offsets = offsets;

        }

        internal static OffsetResponse Decode(KafkaDecoder decoder, string topic)
        {
            var partitionId = decoder.ReadInt32();
            var error = decoder.ReadErrorResponseCode();
            var offsetCount = decoder.ReadInt32();
            var offsets = new long[offsetCount];
            for (int k = 0; k < offsetCount; k++)
            {
                offsets[k] = decoder.ReadInt64();
            }
            var response = new OffsetResponse(topic, partitionId, error, offsets);
            return response;
        }
    }
}
