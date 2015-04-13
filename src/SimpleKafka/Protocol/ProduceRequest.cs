using System;
using System.Collections.Generic;
using System.Linq;
using SimpleKafka.Common;

namespace SimpleKafka.Protocol
{
    public class ProduceRequest : BaseRequest<List<ProduceResponse>>, IKafkaRequest
    {
        /// <summary>
        /// Provide a hint to the broker call not to expect a response for requests without Acks.
        /// </summary>
        public override bool ExpectResponse { get { return Acks > 0; } }
        /// <summary>
        /// Time kafka will wait for requested ack level before returning.
        /// </summary>
        public int TimeoutMS = 1000;
        /// <summary>
        /// Level of ack required by kafka.  0 immediate, 1 written to leader, 2+ replicas synced, -1 all replicas
        /// </summary>
        public Int16 Acks = 1;
        /// <summary>
        /// Collection of payloads to post to kafka
        /// </summary>
        public List<Payload> Payload = new List<Payload>();

        public ProduceRequest() : base(ApiKeyRequestType.Produce) { }

        internal override KafkaEncoder Encode(KafkaEncoder encoder)
        {
            return EncodeProduceRequest(this, encoder);
        }

        internal override List<ProduceResponse> Decode(KafkaDecoder decoder)
        {
            return DecodeProduceResponse(decoder);
        }

        #region Protocol...
        private static KafkaEncoder EncodeProduceRequest(ProduceRequest request, KafkaEncoder encoder)
        {
            request.EncodeHeader(encoder)
                .Write(request.Acks)
                .Write(request.TimeoutMS);

            if (request.Payload == null)
            {
                encoder.Write(0);
            }
            else if (request.Payload.Count == 1)
            {
                // Short cut single request
                var payload = request.Payload[0];
                encoder
                    .Write(1)
                    .Write(payload.Topic)
                    .Write(1);

                WritePayload(encoder, payload);
            }
            else
            {
                // More complex
                var topicGroups = new Dictionary<string, List<Payload>>();
                foreach (var payload in request.Payload)
                {
                    var payloads = topicGroups.GetOrCreate(payload.Topic, () => new List<Payload>(request.Payload.Count));
                    payloads.Add(payload);
                }

                encoder.Write(topicGroups.Count);
                foreach (var kvp in topicGroups)
                {
                    var topic = kvp.Key;
                    var payloads = kvp.Value;

                    encoder
                        .Write(topic)
                        .Write(payloads.Count);

                    foreach (var payload in payloads)
                    {
                        WritePayload(encoder, payload);
                    }
                }
            }
            return encoder;
        }

        private static void WritePayload(KafkaEncoder encoder, Payload payload)
        {
            encoder
                .Write(payload.Partition);

            var marker = encoder.PrepareForLength();
            switch (payload.Codec)
            {
                case MessageCodec.CodecNone:
                    Message.EncodeMessageSet(encoder, payload.Messages);
                    break;

                default:
                    throw new NotSupportedException(string.Format("Codec type of {0} is not supported.", payload.Codec));

            }
            encoder.WriteLength(marker);
        }

        private List<ProduceResponse> DecodeProduceResponse(KafkaDecoder decoder)
        {
            var correlationId = decoder.ReadInt32();

            var responses = new List<ProduceResponse>();
            var topicCount = decoder.ReadInt32();
            for (int i = 0; i < topicCount; i++)
            {
                var topic = decoder.ReadString();

                var partitionCount = decoder.ReadInt32();
                for (int j = 0; j < partitionCount; j++)
                {
                    var response = ProduceResponse.Decode(decoder, topic);
                    responses.Add(response);
                }
            }
            return responses;
        }
        #endregion
    }

    public class ProduceResponse
    {
        /// <summary>
        /// The topic the offset came from.
        /// </summary>
        public readonly string Topic;
        /// <summary>
        /// The partition the offset came from.
        /// </summary>
        public readonly int PartitionId;
        /// <summary>
        /// Error response code.  0 is success.
        /// </summary>
        public readonly ErrorResponseCode Error;
        /// <summary>
        /// The offset number to commit as completed.
        /// </summary>
        public readonly long Offset;

        private ProduceResponse(string topic, int partitionId, ErrorResponseCode error, long offset)
        {
            this.Topic = topic;
            this.PartitionId = partitionId;
            this.Error = error;
            this.Offset = offset;
        }

        internal static ProduceResponse Decode(KafkaDecoder decoder, string topic)
        {
            var partitionId = decoder.ReadInt32();
            var error = decoder.ReadErrorResponseCode();
            var offset = decoder.ReadInt64();

            var response = new ProduceResponse(topic, partitionId, error, offset);
            return response;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((ProduceResponse)obj);
        }

        protected bool Equals(ProduceResponse other)
        {
            return string.Equals(Topic, other.Topic) && PartitionId == other.PartitionId && Error == other.Error && Offset == other.Offset;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hashCode = (Topic != null ? Topic.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ PartitionId;
                hashCode = (hashCode * 397) ^ Error.GetHashCode();
                hashCode = (hashCode * 397) ^ Offset.GetHashCode();
                return hashCode;
            }
        }
    }
}