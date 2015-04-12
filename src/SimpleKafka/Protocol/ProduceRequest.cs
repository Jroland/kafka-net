using System;
using System.Collections.Generic;
using System.Linq;
using SimpleKafka.Common;

namespace SimpleKafka.Protocol
{
    public class ProduceRequest : BaseRequest, IKafkaRequest<List<ProduceResponse>>
    {
        /// <summary>
        /// Provide a hint to the broker call not to expect a response for requests without Acks.
        /// </summary>
        public override bool ExpectResponse { get { return Acks > 0; } }
        /// <summary>
        /// Indicates the type of kafka encoding this request is.
        /// </summary>
        public ApiKeyRequestType ApiKey { get { return ApiKeyRequestType.Produce; } }
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


        public void Encode(ref KafkaEncoder encoder)
        {
            EncodeProduceRequest(this, ref encoder);
        }

        public List<ProduceResponse> Decode(ref KafkaDecoder decoder)
        {
            return DecodeProduceResponse(ref decoder);
        }

        #region Protocol...
        private static void EncodeProduceRequest(ProduceRequest request, ref KafkaEncoder encoder)
        {
            if (request.Payload == null) request.Payload = new List<Payload>();

            var groupedPayloads = (from p in request.Payload
                                   group p by new
                                   {
                                       p.Topic,
                                       p.Partition,
                                       p.Codec
                                   } into tpc
                                   select tpc).ToList();

            EncodeHeader(request, ref encoder);
            encoder.Write(request.Acks);
            encoder.Write(request.TimeoutMS);
            encoder.Write(groupedPayloads.Count);
            foreach (var groupedPayload in groupedPayloads)
            {
                var payloads = groupedPayload.ToList();
                encoder.Write(groupedPayload.Key.Topic, StringPrefixEncoding.Int16);
                encoder.Write(payloads.Count);
                encoder.Write(groupedPayload.Key.Partition);

                var marker = encoder.PrepareForLength();
                switch (groupedPayload.Key.Codec)
                {
                    case MessageCodec.CodecNone:
                        Message.EncodeMessageSet(ref encoder, (payloads.SelectMany(x => x.Messages)));
                        break;
                    default:
                        throw new NotSupportedException(string.Format("Codec type of {0} is not supported.", groupedPayload.Key.Codec));
                }
                encoder.WriteLength(marker);
            }
        }

        private List<ProduceResponse> DecodeProduceResponse(ref KafkaDecoder decoder)
        {
            var correlationId = decoder.ReadInt32();

            var responses = new List<ProduceResponse>();
            var topicCount = decoder.ReadInt32();
            for (int i = 0; i < topicCount; i++)
            {
                var topic = decoder.ReadInt16String();

                var partitionCount = decoder.ReadInt32();
                for (int j = 0; j < partitionCount; j++)
                {
                    var response = new ProduceResponse()
                    {
                        Topic = topic,
                        PartitionId = decoder.ReadInt32(),
                        Error = decoder.ReadInt16(),
                        Offset = decoder.ReadInt64()
                    };

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
        public string Topic { get; set; }
        /// <summary>
        /// The partition the offset came from.
        /// </summary>
        public int PartitionId { get; set; }
        /// <summary>
        /// Error response code.  0 is success.
        /// </summary>
        public Int16 Error { get; set; }
        /// <summary>
        /// The offset number to commit as completed.
        /// </summary>
        public long Offset { get; set; }

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