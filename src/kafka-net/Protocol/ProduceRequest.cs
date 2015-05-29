using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using KafkaNet.Common;
using KafkaNet.Statistics;

namespace KafkaNet.Protocol
{
    public class ProduceRequest : BaseRequest, IKafkaRequest<ProduceResponse>
    {
        /// <summary>
        /// Provide a hint to the broker call not to expect a response for requests without Acks.
        /// </summary>
        public override bool ExpectResponse { get { return Acks != 0; } }
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


        public KafkaDataPayload Encode()
        {
            return EncodeProduceRequest(this);
        }

        public IEnumerable<ProduceResponse> Decode(byte[] payload)
        {
            return DecodeProduceResponse(payload);
        }

        #region Protocol...
        private KafkaDataPayload EncodeProduceRequest(ProduceRequest request)
        {
            int totalCompressedBytes = 0;
            if (request.Payload == null) request.Payload = new List<Payload>();

            var groupedPayloads = (from p in request.Payload
                                   group p by new
                                   {
                                       p.Topic,
                                       p.Partition,
                                       p.Codec
                                   } into tpc
                                   select tpc).ToList();

            using (var message = EncodeHeader(request)
                .Pack(request.Acks)
                .Pack(request.TimeoutMS)
                .Pack(groupedPayloads.Count))
            {
                foreach (var groupedPayload in groupedPayloads)
                {
                    var payloads = groupedPayload.ToList();
                    message.Pack(groupedPayload.Key.Topic, StringPrefixEncoding.Int16)
                        .Pack(payloads.Count)
                        .Pack(groupedPayload.Key.Partition);

                    switch (groupedPayload.Key.Codec)
                    {

                        case MessageCodec.CodecNone:
                            message.Pack(Message.EncodeMessageSet(payloads.SelectMany(x => x.Messages)));
                            break;
                        case MessageCodec.CodecGzip:
                            var compressedBytes = CreateGzipCompressedMessage(payloads.SelectMany(x => x.Messages));
                            Interlocked.Add(ref totalCompressedBytes, compressedBytes.CompressedAmount);
                            message.Pack(Message.EncodeMessageSet(new[] { compressedBytes.CompressedMessage }));
                            break;
                        default:
                            throw new NotSupportedException(string.Format("Codec type of {0} is not supported.", groupedPayload.Key.Codec));
                    }
                }

                var result = new KafkaDataPayload
                {
                    Buffer = message.Payload(),
                    CorrelationId = request.CorrelationId,
                    MessageCount = request.Payload.Sum(x => x.Messages.Count)
                };
                StatisticsTracker.RecordProduceRequest(result.MessageCount, result.Buffer.Length, totalCompressedBytes);
                return result;
            }
        }

        private CompressedMessageResult CreateGzipCompressedMessage(IEnumerable<Message> messages)
        {
            var messageSet = Message.EncodeMessageSet(messages);

            var gZipBytes = Compression.Zip(messageSet);

            var compressedMessage = new Message
            {
                Attribute = (byte)(0x00 | (ProtocolConstants.AttributeCodeMask & (byte)MessageCodec.CodecGzip)),
                Value = gZipBytes
            };

            return new CompressedMessageResult
            {
                CompressedAmount = messageSet.Length - compressedMessage.Value.Length,
                CompressedMessage = compressedMessage
            };
        }

        private IEnumerable<ProduceResponse> DecodeProduceResponse(byte[] data)
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
                        var response = new ProduceResponse()
                        {
                            Topic = topic,
                            PartitionId = stream.ReadInt32(),
                            Error = stream.ReadInt16(),
                            Offset = stream.ReadInt64()
                        };

                        yield return response;
                    }
                }
            }
        }
        #endregion
    }

    class CompressedMessageResult
    {
        public int CompressedAmount { get; set; }
        public Message CompressedMessage { get; set; }
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
