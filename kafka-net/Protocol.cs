using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Kafka.Common;
using kafka_net.Common;

namespace Kafka
{
    public enum ProtocolEncoding
    {
        Produce = 0,
        Fetch = 1,
        Offset = 2,
        MetaData = 3,
        OffsetCommit = 8,
        OffsetFetch = 9
    }

    public class Protocol
    {
        private const Int16 ApiVersion = 0;

        private readonly static Crc32 Crc32 = new Crc32();

        /// <summary>
        /// Encode the common head for kafka request.
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        /// <remarks>Format: (hhihs) </remarks>
        public byte[] EncodeHeader(IKafkaRequest request)
        {
            var message = new WriteByteStream();

            message.Pack(((Int16)request.EncodingKey).ToBytes(),
                          ApiVersion.ToBytes(),
                          request.CorrelationId.ToBytes(),
                          request.ClientId.ToInt16SizedBytes());

            return message.Payload();
        }

        /// <summary>
        /// Encodes a collection of messages into one byte[].  Encoded in order of list.
        /// </summary>
        /// <param name="messages">The collection of messages to encode together.</param>
        /// <returns>Encoded byte[] representing the collection of messages.</returns>
        public byte[] EncodeMessageSet(IEnumerable<Message> messages)
        {
            var messageSet = new WriteByteStream();

            foreach (var message in messages)
            {
                var encodedMessage = EncodeMessage(message);
                messageSet.Pack(((long)0).ToBytes(), encodedMessage.Length.ToBytes(), encodedMessage);
            }

            return messageSet.Payload();
        }

        /// <summary>
        /// Encodes a message object to byte[]
        /// </summary>
        /// <param name="message">Message data to encode.</param>
        /// <returns>Encoded byte[] representation of the message object.</returns>
        /// <remarks>
        /// Format:
        /// Crc (Int32), MagicByte (Byte), Attributes (Byte), Key (Byte[]), Value (Byte[])
        /// </remarks>
        public byte[] EncodeMessage(Message message)
        {
            var body = new WriteByteStream();

            body.Pack(new[] { message.MagicNumber },
                      new[] { message.Attributes },
                      message.Key.ToIntSizedBytes(),
                      message.Value.ToIntSizedBytes());

            var crc = Crc32.ComputeHash(body.Payload());
            body.Prepend(crc);

            return body.Payload();
        }

        public Message DecodeMessage(byte[] payload)
        {
            var crc = payload.Take(4);
            var stream = new ReadByteStream(payload.Skip(4));

            if (crc.SequenceEqual(Crc32.ComputeHash(stream.Payload)) == false)
                throw new FailCrcCheckException("Payload did not match CRC validation.");

            var message = new Message
                {
                    MagicNumber = stream.ReadByte(),
                    Attributes = stream.ReadByte(),
                    Key = stream.ReadIntString(),
                    Value = stream.ReadIntString()
                };

            return message;
        }

        public byte[] EncodeProduceRequest(ProduceRequest request)
        {
            var message = new WriteByteStream();

            if (request.Payload == null) request.Payload = new List<Payload>();

            var topicGroups = request.Payload.GroupBy(x => x.Topic).ToList();

            message.Pack(EncodeHeader(request)); //header
            message.Pack(request.Acks.ToBytes(), request.TimeoutMS.ToBytes(), topicGroups.Count.ToBytes()); //metadata

            foreach (var topicGroup in topicGroups)
            {
                var partitions = topicGroup.GroupBy(x => x.Partition).ToList();
                message.Pack(topicGroup.Key.ToInt16SizedBytes(), partitions.Count.ToBytes());

                foreach (var partition in partitions)
                {
                    var messageSet = EncodeMessageSet(partition.SelectMany(x => x.Messages));
                    message.Pack(partition.Key.ToBytes(), messageSet.Count().ToBytes(), messageSet);
                }
            }

            //prepend final messages size and return
            message.Prepend(message.Length().ToBytes());

            return message.Payload();
        }

        public byte[] EncodeMetadataRequest(MetadataRequest request)
        {
            var message = new WriteByteStream();

            if (request.Topics == null) request.Topics = new List<string>();

            message.Pack(EncodeHeader(request)); //header
            message.Pack(request.Topics.Count.ToBytes());
            message.Pack(request.Topics.Select(x => x.ToInt16SizedBytes()).ToArray());
            message.Prepend(message.Length().ToBytes());

            return message.Payload();
        }

        public MetadataResponse DecodeMetadataResponse(byte[] data)
        {
            var stream = new ReadByteStream(data);
            var response = new MetadataResponse();
            var correlationId = stream.ReadInt();

            var brokerCount = stream.ReadInt();
            for (var i = 0; i < brokerCount; i++)
            {
                response.Brokers.Add(Broker.FromStream(stream));
            }

            var topicCount = stream.ReadInt();
            for (var i = 0; i < topicCount; i++)
            {
                response.Topics.Add(Topic.FromStream(stream));
            }

            return response;
        }
    }
}
