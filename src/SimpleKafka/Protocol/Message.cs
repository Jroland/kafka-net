using System;
using System.Collections.Generic;
using System.Linq;
using SimpleKafka.Common;

namespace SimpleKafka.Protocol
{
    /// <summary>
    /// Payload represents a collection of messages to be posted to a specified Topic on specified Partition.
    /// </summary>
    public class Payload
    {
        public Payload()
        {
            Codec = MessageCodec.CodecNone;
        }

        public string Topic { get; set; }
        public int Partition { get; set; }
        public MessageCodec Codec { get; set; }
        public IList<Message> Messages { get; set; }
    }

    /// <summary>
    /// Message represents the data from a single event occurance.
    /// </summary>
    public class Message
    {
        private const int MessageHeaderSize = 12;
        private const long InitialMessageOffset = 0;

        /// <summary>
        /// Metadata on source offset and partition location for this message.
        /// </summary>
        public MessageMetadata Meta { get; set; }
        /// <summary>
        /// This is a version id used to allow backwards compatible evolution of the message binary format.  Reserved for future use.  
        /// </summary>
        public byte MagicNumber { get; set; }
        /// <summary>
        /// Attribute value outside message body used for added codec/compression info.
        /// </summary>
        public byte Attribute { get; set; }
        /// <summary>
        /// Key value used for routing message to partitions.
        /// </summary>
        public byte[] Key { get; set; }
        /// <summary>
        /// The message body contents.  Can contain compress message set.
        /// </summary>
        public byte[] Value { get; set; }

        /// <summary>
        /// Construct an empty message.
        /// </summary>
        public Message() { }

        /// <summary>
        /// Convenience constructor will encode both the key and message to byte streams.
        /// Most of the time a message will be string based.
        /// </summary>
        /// <param name="key">The key value for the message.  Can be null.</param>
        /// <param name="value">The main content data of this message.</param>
        public Message(string value, string key = null)
        {
            Key = key == null ? null : key.ToBytes();
            Value = value.ToBytes();
        }

        /// <summary>
        /// Encodes a collection of messages into one byte[].  Encoded in order of list.
        /// </summary>
        /// <param name="messages">The collection of messages to encode together.</param>
        /// <returns>Encoded byte[] representing the collection of messages.</returns>
        internal static KafkaEncoder EncodeMessageSet(KafkaEncoder encoder, IEnumerable<Message> messages)
        {
            foreach (var message in messages)
            {
                encoder.Write(InitialMessageOffset);
                var marker = encoder.PrepareForLength();
                EncodeMessage(message, encoder)
                    .WriteLength(marker);
            }
            return encoder;
        }

        /// <summary>
        /// Decode a byte[] that represents a collection of messages.
        /// </summary>
        /// <param name="decoder">The decoder positioned at the start of the buffer</param>
        /// <returns>The messages</returns>
        internal static List<Message> DecodeMessageSet(int partitionId, KafkaDecoder decoder, int messageSetSize)
        {
            var numberOfBytes = messageSetSize;

            var messages = new List<Message>();
            while (numberOfBytes > 0)
            {

                if (numberOfBytes < MessageHeaderSize)
                {
                    break;
                }
                var offset = decoder.ReadInt64();
                var messageSize = decoder.ReadInt32();
                if (messageSetSize - MessageHeaderSize < messageSize)
                {
                    // This message is too big to fit in the buffer so we will never get it
                    throw new BufferUnderRunException(numberOfBytes, messageSize);
                }

                numberOfBytes -= MessageHeaderSize;
                if (numberOfBytes < messageSize)
                {
                    break;
                }

                var message = DecodeMessage(offset, partitionId, decoder, messageSize);
                messages.Add(message);
                numberOfBytes -= messageSize;
            }
            return messages;
        }

        /// <summary>
        /// Encodes a message object to byte[]
        /// </summary>
        /// <param name="message">Message data to encode.</param>
        /// <returns>Encoded byte[] representation of the message object.</returns>
        /// <remarks>
        /// Format:
        /// Crc (Int32), MagicByte (Byte), Attribute (Byte), Key (Byte[]), Value (Byte[])
        /// </remarks>
        internal static KafkaEncoder EncodeMessage(Message message, KafkaEncoder encoder)
        {
            var marker = encoder.PrepareForCrc();
            encoder
                .Write(message.MagicNumber)
                .Write(message.Attribute)
                .Write(message.Key)
                .Write(message.Value)
                .CalculateCrc(marker);

            return encoder;
        }

        /// <summary>
        /// Decode messages from a payload and assign it a given kafka offset.
        /// </summary>
        /// <param name="offset">The offset represting the log entry from kafka of this message.</param>
        /// <param name="payload">The byte[] encode as a message from kafka.</param>
        /// <returns>The message</returns>
        /// <remarks>The return type is an Enumerable as the message could be a compressed message set.</remarks>
        internal static Message DecodeMessage(long offset, int partitionId, KafkaDecoder decoder, int messageSize)
        {
            var crc = decoder.ReadUInt32();
            var calculatedCrc = Crc32Provider.Compute(decoder.Buffer, decoder.Offset, messageSize - 4);
            if (calculatedCrc != crc)
            {
                throw new FailCrcCheckException("Payload did not match CRC validation.");
            }

            var message = new Message
            {
                Meta = new MessageMetadata(offset, partitionId),
                MagicNumber = decoder.ReadByte(),
                Attribute = decoder.ReadByte(),
                Key = decoder.ReadBytes(),
                
            };
            var codec = (MessageCodec)(ProtocolConstants.AttributeCodeMask & message.Attribute);
            switch (codec)
            {
                case MessageCodec.CodecNone:
                    message.Value = decoder.ReadBytes();
                    break;

                default:
                    throw new NotSupportedException(string.Format("Codec type of {0} is not supported.", codec));
            }
            return message;
        }
    }

    /// <summary>
    /// Provides metadata about the message received from the FetchResponse
    /// </summary>
    /// <remarks>
    /// The purpose of this metadata is to allow client applications to track their own offset information about messages received from Kafka.
    /// <see cref="http://kafka.apache.org/documentation.html#semantics"/>
    /// </remarks>
    public class MessageMetadata
    {
        /// <summary>
        /// The log offset of this message as stored by the Kafka server.
        /// </summary>
        public readonly long Offset;
        /// <summary>
        /// The partition id this offset is from.
        /// </summary>
        public readonly int PartitionId;

        public MessageMetadata(long offset, int partitionId)
        {
            this.PartitionId = partitionId;
            this.Offset = offset;
        }
    }
}
