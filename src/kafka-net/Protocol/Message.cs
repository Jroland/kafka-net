﻿using System;
using System.Collections.Generic;
using System.Linq;
using KafkaNet.Common;

namespace KafkaNet.Protocol
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
        public List<Message> Messages { get; set; }
    }

    /// <summary>
    /// Message represents the data from a single event occurance.
    /// </summary>
    public class Message
    {
        private const int MessageHeaderSize = 12;
        private static readonly Crc32 Crc32 = new Crc32();

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
        public static byte[] EncodeMessageSet(IEnumerable<Message> messages)
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
        /// Decode a byte[] that represents a collection of messages.
        /// </summary>
        /// <param name="messageSet">The byte[] encode as a message set from kafka.</param>
        /// <returns>Enumerable representing stream of messages decoded from byte[]</returns>
        public static IEnumerable<Message> DecodeMessageSet(byte[] messageSet)
        {
            var stream = new ReadByteStream(messageSet);


            while (stream.HasData)
            {
                //this checks that we have at least the minimum amount of data to retrieve a header
                if (stream.Available(MessageHeaderSize) == false)
                    yield break;

                var offset = stream.ReadLong();
                var messageSize = stream.ReadInt();

               
                //if messagessize is greater than payload, our max buffer is insufficient.
                if (stream.Payload.Length < messageSize)
                    throw new BufferUnderRunException(messageSize + MessageHeaderSize);

                //if the stream does not have enough left in the payload, we got only a partial message
                if (stream.Available(messageSize) == false)
                    yield break;

                foreach (var message in DecodeMessage(offset, stream.ReadBytesFromStream(messageSize)))
                {
                    yield return message;
                }
            }
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
        public static byte[] EncodeMessage(Message message)
        {
            var body = new WriteByteStream();

            body.Pack(new[] { message.MagicNumber },
                      new[] { message.Attribute },
                      message.Key.ToIntPrefixedBytes(),
                      message.Value.ToIntPrefixedBytes());

            var crc = Crc32.ComputeHash(body.Payload());
            body.Prepend(crc);

            return body.Payload();
        }

        /// <summary>
        /// Decode messages from a payload and assign it a given kafka offset.
        /// </summary>
        /// <param name="offset">The offset represting the log entry from kafka of this message.</param>
        /// <param name="payload">The byte[] encode as a message from kafka.</param>
        /// <returns>Enumerable representing stream of messages decoded from byte[].</returns>
        /// <remarks>The return type is an Enumerable as the message could be a compressed message set.</remarks>
        public static IEnumerable<Message> DecodeMessage(long offset, byte[] payload)
        {
            var crc = payload.Take(4);
            var stream = new ReadByteStream(payload.Skip(4));

            if (crc.SequenceEqual(Crc32.ComputeHash(stream.Payload)) == false)
                throw new FailCrcCheckException("Payload did not match CRC validation.");

            var message = new Message
            {
                Meta = new MessageMetadata { Offset = offset },
                MagicNumber = stream.ReadByte(),
                Attribute = stream.ReadByte(),
                Key = stream.ReadIntPrefixedBytes()
            };

            var codec = (MessageCodec)(ProtocolConstants.AttributeCodeMask & message.Attribute);
            switch (codec)
            {
                case MessageCodec.CodecNone:
                    message.Value = stream.ReadIntPrefixedBytes();
                    yield return message;
                    break;
                case MessageCodec.CodecGzip:
                    var gZipData = stream.ReadIntPrefixedBytes();
                    foreach (var m in DecodeMessageSet(Compression.Unzip(gZipData)))
                    {
                        yield return m;
                    }
                    break;
                default:
                    throw new NotSupportedException(string.Format("Codec type of {0} is not supported.", codec));
            }
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
        public long Offset { get; set; }
        /// <summary>
        /// The partition id this offset is from.
        /// </summary>
        public int PartitionId { get; set; }
    }
}
