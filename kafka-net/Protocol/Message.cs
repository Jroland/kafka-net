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
        public string Topic { get; set; }
        public int Partition { get; set; }
        public List<Message> Messages { get; set; }
    }

    /// <summary>
    /// Message represents the data from a single event occurance.
    /// </summary>
    public class Message
    {
        private static readonly Crc32 Crc32 = new Crc32();

        /// <summary>
        /// The offset of the message within the kafka log.
        /// </summary>
        public long Offset { get; set; }
        public byte MagicNumber { get; set; }
        /// <summary>
        /// Attribute value outside message body used for added codec/compression info.
        /// </summary>
        public byte Attribute { get; set; }
        /// <summary>
        /// Key value used for routing message to partitions.
        /// </summary>
        public string Key { get; set; }
        /// <summary>
        /// The message body contents.  Can contain compress message set.
        /// </summary>
        public string Value { get; set; }

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
                var offset = stream.ReadLong();
                foreach (var message in DecodeMessage(offset, stream.ReadIntPrefixedBytes()))
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
                      message.Key.ToIntSizedBytes(),
                      message.Value.ToIntSizedBytes());

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
                Offset = offset,
                MagicNumber = stream.ReadByte(),
                Attribute = stream.ReadByte(),
                Key = stream.ReadIntString(),
                Value = stream.ReadIntString()
            };

            //TODO if message compressed then expand more messages
            //use attribute to determine compression

            yield return message;
        }
    }
}
