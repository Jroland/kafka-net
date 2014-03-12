using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using KafkaNet.Common;

namespace KafkaNet.Protocol
{
    public class ProduceRequest : BaseRequest, IKafkaRequest<ProduceResponse>
    {
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

        /// <summary>
        /// Copy of this ProduceRequest with an empty Payload collection.
        /// </summary>
        /// <returns>A ProduceRequest without empty Payload collection.</returns>
        public ProduceRequest Copy()
        {
            return new ProduceRequest
                {
                    TimeoutMS = this.TimeoutMS,
                    Acks = this.Acks,
                    Payload = new List<Payload>()
                };
        }

        public byte[] Encode()
        {
            return EncodeProduceRequest(this);
        }

        public IEnumerable<ProduceResponse> Decode(byte[] payload)
        {
            return DecodeProduceResponse(payload);
        }

        #region Protocol...
        private byte[] EncodeProduceRequest(ProduceRequest request)
        {
            var message = new WriteByteStream();

            if (request.Payload == null) request.Payload = new List<Payload>();

            var topicGroups = request.Payload.GroupBy(x => x.Topic).ToList();

            message.Pack(EncodeHeader(request)); //header
            message.Pack(request.Acks.ToBytes(), request.TimeoutMS.ToBytes(), topicGroups.Count.ToBytes()); //metadata

            var groupedPayloads = (from p in request.Payload
                                   group p by new
                                       {
                                           p.Topic,
                                           p.Partition,
                                           p.Codec
                                       }
                                       into tpc
                                       select tpc).ToList();

            foreach (var groupedPayload in groupedPayloads)
            {
                var payloads = groupedPayload.ToList();
                message.Pack(groupedPayload.Key.Topic.ToInt16SizedBytes(), payloads.Count.ToBytes());

                byte[] messageSet;
                switch (groupedPayload.Key.Codec)
                {
                    case MessageCodec.CodecNone:
                        messageSet = Message.EncodeMessageSet(payloads.SelectMany(x => x.Messages));
                        break;
                    case MessageCodec.CodecGzip:
                        messageSet = Message.EncodeMessageSet(CompressWithGzip(payloads.SelectMany(x => x.Messages)));
                        break;
                    default:
                        throw new NotSupportedException(string.Format("Codec type of {0} is not supported.", groupedPayload.Key.Codec));
                }

                message.Pack(groupedPayload.Key.Partition.ToBytes(), messageSet.Count().ToBytes(), messageSet);
            }
            
            //prepend final messages size and return
            message.Prepend(message.Length().ToBytes());

            return message.Payload();
        }

        private IEnumerable<Message> CompressWithGzip(IEnumerable<Message> messages)
        {
            var messageSet = Message.EncodeMessageSet(messages);

            var ms = new MemoryStream();
            using (var gZipStream = new GZipStream(ms, CompressionMode.Compress, false))
            {
                gZipStream.Write(messageSet, 0, messageSet.Length);
                gZipStream.Flush();
                
                var compressedMessage = new Message
                    {
                        Attribute = (byte)(0x00 | (ProtocolConstants.AttributeCodeMask & (byte)MessageCodec.CodecGzip)),
                        Value = Encoding.ASCII.GetString(ms.ToArray())
                    };

                return new[] { compressedMessage };
            }
        }

        private IEnumerable<ProduceResponse> DecodeProduceResponse(byte[] data)
        {
            var stream = new ReadByteStream(data);

            var correlationId = stream.ReadInt();

            var topicCount = stream.ReadInt();
            for (int i = 0; i < topicCount; i++)
            {
                var topic = stream.ReadInt16String();

                var partitionCount = stream.ReadInt();
                for (int j = 0; j < partitionCount; j++)
                {
                    var response = new ProduceResponse()
                    {
                        Topic = topic,
                        PartitionId = stream.ReadInt(),
                        Error = stream.ReadInt16(),
                        Offset = stream.ReadLong()
                    };

                    yield return response;
                }
            }
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
        /// The offset number to commit as completed.
        /// </summary>
        public Int16 Error { get; set; }
        public long Offset { get; set; }
    }
}