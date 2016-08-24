using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using kafka_tests.Helpers;
using KafkaNet.Common;
using KafkaNet.Protocol;
using NUnit.Framework;

namespace kafka_tests.Unit
{
    /// <summary>
    /// From http://kafka.apache.org/protocol.html#protocol_types
    /// The protocol is built out of the following primitive types.
    ///
    /// Fixed Width Primitives:
    /// int8, int16, int32, int64 - Signed integers with the given precision (in bits) stored in big endian order.
    ///
    /// Variable Length Primitives:
    /// bytes, string - These types consist of a signed integer giving a length N followed by N bytes of content. 
    /// A length of -1 indicates null. string uses an int16 for its size, and bytes uses an int32.
    ///
    /// Arrays:
    /// This is a notation for handling repeated structures. These will always be encoded as an int32 size containing 
    /// the length N followed by N repetitions of the structure which can itself be made up of other primitive types. 
    /// In the BNF grammars below we will show an array of a structure foo as [foo].
    /// 
    /// Message formats are from http://kafka.apache.org/protocol.html#protocol_messages
    /// 
    /// Request Header => api_key api_version correlation_id client_id 
    ///  api_key => INT16             -- The id of the request type.
    ///  api_version => INT16         -- The version of the API.
    ///  correlation_id => INT32      -- A user-supplied integer value that will be passed back with the response.
    ///  client_id => NULLABLE_STRING -- A user specified identifier for the client making the request.
    /// 
    /// Response Header => correlation_id 
    ///  correlation_id => INT32      -- The user-supplied value passed in with the request
    /// </summary>
    [TestFixture]
    [Category("Unit")]
    public class ProtocolMessageTests
    {
        /// <summary>
        /// Produce Request (Version: 0,1,2) => acks timeout [topic_data] 
        ///  acks => INT16                   -- The number of nodes that should replicate the produce before returning. -1 indicates the full ISR.
        ///  timeout => INT32                -- The time to await a response in ms.
        ///  topic_data => topic [data] 
        ///    topic => STRING
        ///    data => partition record_set 
        ///      partition => INT32
        ///      record_set => BYTES
        /// 
        /// where:
        /// record_set => MessageSetSize MessageSet
        ///  MessageSetSize => int32
        /// 
        /// NB. MessageSets are not preceded by an int32 like other array elements in the protocol:
        /// MessageSet => [Offset MessageSize Message]
        ///  Offset => int64      -- This is the offset used in kafka as the log sequence number. When the producer is sending non compressed messages, 
        ///                          it can set the offsets to anything. When the producer is sending compressed messages, to avoid server side recompression, 
        ///                          each compressed message should have offset starting from 0 and increasing by one for each inner message in the compressed message. 
        ///  MessageSize => int32
        /// 
        /// Message => Crc MagicByte Attributes Timestamp Key Value
        ///   Crc => int32       -- The CRC is the CRC32 of the remainder of the message bytes. This is used to check the integrity of the message on the broker and consumer.
        ///   MagicByte => int8  -- This is a version id used to allow backwards compatible evolution of the message binary format. The current value is 1.
        ///   Attributes => int8 -- This byte holds metadata attributes about the message.
        ///                         The lowest 3 bits contain the compression codec used for the message.
        ///                         The fourth lowest bit represents the timestamp type. 0 stands for CreateTime and 1 stands for LogAppendTime. The producer should always set this bit to 0. (since 0.10.0)
        ///                         All other bits should be set to 0.
        ///   Timestamp => int64 -- ONLY version 1! This is the timestamp of the message. The timestamp type is indicated in the attributes. Unit is milliseconds since beginning of the epoch (midnight Jan 1, 1970 (UTC)).
        ///   Key => bytes       -- The key is an optional message key that was used for partition assignment. The key can be null.
        ///   Value => bytes     -- The value is the actual message contents as an opaque byte array. Kafka supports recursive messages in which case this may itself contain a message set. The message can be null.
        /// </summary>
        [Test]
        public void ProduceApiRequestBytes(
            [Values(0)] short version, // currently only supported version
            [Values(0, 1, 2, -1)] short acks, 
            [Values(0, 1, 1000)] int timeoutMilliseconds, 
            [Values("test", "a really long name, with spaces and punctuation!")] string topic, 
            [Values(1, 10)] int topicsPerRequest, 
            [Values(1)] int partitionsPerTopic, // client only supports 1
            [Values(1, 5)] int totalPartitions, 
            [Values(1, 2, 3)] int messagesPerSet)
        {
            var randomizer = new Randomizer();
            var clientId = nameof(ProduceApiRequestBytes);

            var request = new ProduceRequest {
                Acks = acks,
                ClientId = clientId,
                CorrelationId = clientId.GetHashCode(),
                TimeoutMS = timeoutMilliseconds,
                Payload = new List<Payload>()
            };

            for (var t = 0; t < topicsPerRequest; t++) {
                var payload = new Payload {
                    Topic = topic + t,
                    Partition = t % totalPartitions,
                    Codec = MessageCodec.CodecNone,
                    Messages = new List<Message>()
                };
                for (var m = 0; m < messagesPerSet; m++) {
                    var message = new Message {
                        MagicNumber = 1,
                        Key = m > 0 ? new byte[8] : null,
                        Value = new byte[8*(m + 1)]
                    };
                    if (message.Key != null) {
                        randomizer.NextBytes(message.Key);
                    }
                    randomizer.NextBytes(message.Value);
                    payload.Messages.Add(message);
                }
                request.Payload.Add(payload);
            }

            var data = request.Encode();

            AssertProtocolBytes(data.Buffer,
                reader => {
                    AssertRequestHeader(request.ApiKey, version, request.CorrelationId, request.ClientId, reader);

                    Assert.That(reader.ReadInt16(), Is.EqualTo(request.Acks), "acks");
                    Assert.That(reader.ReadInt32(), Is.EqualTo(request.TimeoutMS), "timeout");

                    Assert.That(reader.ReadInt32(), Is.EqualTo(request.Payload.Count), "[topic_data]");
                    foreach (var payload in request.Payload) {
                        Assert.That(reader.ReadString(), Is.EqualTo(payload.Topic), "TopicName");
                        Assert.That(reader.ReadInt32(), Is.EqualTo(partitionsPerTopic), "[Partition]");
                        Assert.That(reader.ReadInt32(), Is.EqualTo(payload.Partition), "Partition");

                        var finalPosition = reader.ReadInt32() + reader.Position;
                        AssertMessageSet(payload.Messages.Select(m => 
                            new Tuple<byte, DateTime?, byte[], byte[]>(m.Attribute, version == 2 ? DateTime.MinValue : (DateTime?) null, m.Key, m.Value)), reader);
                        Assert.That(reader.Position, Is.EqualTo(finalPosition), "MessageSetSize");
                    }
                });
        }

        private static DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        /// <summary>
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets
        /// 
        /// NB. MessageSets are not preceded by an int32 like other array elements in the protocol:
        /// MessageSet => [Offset MessageSize Message]
        ///  Offset => int64      -- This is the offset used in kafka as the log sequence number. When the producer is sending non compressed messages, 
        ///                          it can set the offsets to anything. When the producer is sending compressed messages, to avoid server side recompression, 
        ///                          each compressed message should have offset starting from 0 and increasing by one for each inner message in the compressed message. 
        ///  MessageSize => int32
        /// </summary>
        private static void AssertMessageSet(IEnumerable<Tuple<byte, DateTime?, byte[], byte[]>> messageValues, BigEndianBinaryReader reader)
        {
            foreach (var value in messageValues) {
                var offset = reader.ReadInt64();
                if (value.Item1 != (byte)MessageCodec.CodecNone) {
                    // TODO: assert offset?
                }
                var finalPosition = reader.ReadInt32() + reader.Position;
                AssertMessage(value.Item1, value.Item2, value.Item3, value.Item4, reader);
                Assert.That(reader.Position, Is.EqualTo(finalPosition), "MessageSize");
            }
        }

        /// <summary>
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets
        /// 
        /// Message => Crc MagicByte Attributes Timestamp Key Value
        ///   Crc => int32       -- The CRC is the CRC32 of the remainder of the message bytes. This is used to check the integrity of the message on the broker and consumer.
        ///   MagicByte => int8  -- This is a version id used to allow backwards compatible evolution of the message binary format. The current value is 1.
        ///   Attributes => int8 -- This byte holds metadata attributes about the message.
        ///                         The lowest 3 bits contain the compression codec used for the message.
        ///                         The fourth lowest bit represents the timestamp type. 0 stands for CreateTime and 1 stands for LogAppendTime. The producer should always set this bit to 0. (since 0.10.0)
        ///                         All other bits should be set to 0.
        ///   Timestamp => int64 -- ONLY version 1! This is the timestamp of the message. The timestamp type is indicated in the attributes. Unit is milliseconds since beginning of the epoch (midnight Jan 1, 1970 (UTC)).
        ///   Key => bytes       -- The key is an optional message key that was used for partition assignment. The key can be null.
        ///   Value => bytes     -- The value is the actual message contents as an opaque byte array. Kafka supports recursive messages in which case this may itself contain a message set. The message can be null.
        /// </summary>
        private static void AssertMessage(byte attributes, DateTime? timestamp, byte[] key, byte[] value, BigEndianBinaryReader reader)
        {
            var crc = (uint)reader.ReadInt32();
            var positionAfterCrc = reader.Position;
            Assert.That(reader.ReadByte(), Is.EqualTo((byte)1), "MagicByte");
            Assert.That(reader.ReadByte(), Is.EqualTo((byte)attributes), "Attributes");
            if (timestamp.HasValue) {
                Assert.That(reader.ReadInt64(), Is.EqualTo((timestamp.Value - UnixEpoch).TotalMilliseconds), "Timestamp");
            }
            Assert.That(reader.ReadBytes(), Is.EqualTo(key), "Key");
            Assert.That(reader.ReadBytes(), Is.EqualTo(value), "Value");

            var positionAfterMessage = reader.Position;
            reader.Position = positionAfterCrc;
            var messageBytes = reader.ReadBytes((int) (positionAfterMessage - positionAfterCrc));
            Assert.That(Crc32Provider.Compute(messageBytes), Is.EqualTo(crc));
            reader.Position = positionAfterMessage;
        }

        /// <summary>
        /// From http://kafka.apache.org/protocol.html#protocol_messages
        /// 
        /// Request Header => api_key api_version correlation_id client_id 
        ///  api_key => INT16             -- The id of the request type.
        ///  api_version => INT16         -- The version of the API.
        ///  correlation_id => INT32      -- A user-supplied integer value that will be passed back with the response.
        ///  client_id => NULLABLE_STRING -- A user specified identifier for the client making the request.
        /// </summary>
        private static void AssertRequestHeader(ApiKeyRequestType apiKey, short version, int correlationId, string clientId, BigEndianBinaryReader reader)
        {
            // Request Header
            Assert.That(reader.ReadInt16(), Is.EqualTo((short) apiKey), "api_key");
            Assert.That(reader.ReadInt16(), Is.EqualTo(version), "api_version");
            Assert.That(reader.ReadInt32(), Is.EqualTo(correlationId), "correlation_id");
            Assert.That(reader.ReadString(), Is.EqualTo(clientId), "client_id");
        }

        /// <summary>
        /// From http://kafka.apache.org/protocol.html#protocol_messages
        /// 
        /// Response Header => correlation_id 
        ///  correlation_id => INT32      -- The user-supplied value passed in with the request
        /// </summary>
        private void AssertResponseMessage(byte[] bytes, int correlationId, Action<BigEndianBinaryReader> assertResponse)
        {
            AssertProtocolBytes(bytes, reader => {
                // Response Header
                Assert.That(reader.ReadInt32(), Is.EqualTo(correlationId));
                
                // Response
                assertResponse(reader);
            });
        }

        /// <summary>
        /// From http://kafka.apache.org/protocol.html#protocol_common
        /// 
        /// RequestOrResponse => Size (RequestMessage | ResponseMessage)
        ///  Size => int32 -- The message_size field gives the size of the subsequent request or response message in bytes. 
        ///                   The client can read requests by first reading this 4 byte size as an integer N, and then reading 
        ///                   and parsing the subsequent N bytes of the request.
        /// </summary>
        private void AssertProtocolBytes(byte[] bytes, Action<BigEndianBinaryReader> assert)
        {
            using (var reader = new BigEndianBinaryReader(bytes)) {
                Assert.That(reader.ReadInt32(), Is.EqualTo(reader.Length - 4), "Size");
                assert(reader);
                Assert.That(reader.HasData, Is.False);
            }
        }

        [Test]
        [ExpectedException(typeof(FailCrcCheckException))]
        public void DecodeMessageShouldThrowWhenCrcFails()
        {
            var testMessage = new Message(value: "kafka test message.", key: "test");

            var encoded = Message.EncodeMessage(testMessage);
            encoded[0] += 1;
            var result = Message.DecodeMessage(0, encoded).First();
        }

        [Test]
        [TestCase("test key", "test message")]
        [TestCase(null, "test message")]
        [TestCase("test key", null)]
        [TestCase(null, null)]
        public void EnsureMessageEncodeAndDecodeAreCompatible(string key, string value)
        {
            var testMessage = new Message(key: key, value: value);

            var encoded = Message.EncodeMessage(testMessage);
            var result = Message.DecodeMessage(0, encoded).First();

            Assert.That(testMessage.Key, Is.EqualTo(result.Key));
            Assert.That(testMessage.Value, Is.EqualTo(result.Value));
        }

        [Test]
        public void EncodeMessageSetEncodesMultipleMessages()
        {
            //expected generated from python library
            var expected = new byte[]
                {
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 45, 70, 24, 62, 0, 0, 0, 0, 0, 1, 49, 0, 0, 0, 1, 48, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 16, 90, 65, 40, 168, 0, 0, 0, 0, 0, 1, 49, 0, 0, 0, 1, 49, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 16, 195, 72, 121, 18, 0, 0, 0, 0, 0, 1, 49, 0, 0, 0, 1, 50
                };

            var messages = new[]
                {
                    new Message("0", "1"),
                    new Message("1", "1"),
                    new Message("2", "1")
                };

            var result = Message.EncodeMessageSet(messages);

            Assert.That(expected, Is.EqualTo(result));
        }

        [Test]
        public void DecodeMessageSetShouldHandleResponseWithMaxBufferSizeHit()
        {
            //This message set has a truncated message bytes at the end of it
            var result = Message.DecodeMessageSet(MessageHelper.FetchResponseMaxBytesOverflow).ToList();

            var message = Encoding.UTF8.GetString(result.First().Value);
            
            Assert.That(message, Is.EqualTo("test"));
            Assert.That(result.Count, Is.EqualTo(529));
        }

        [Test]
        public void WhenMessageIsTruncatedThenBufferUnderRunExceptionIsThrown()
        {
            // arrange
            var offset = (Int64)0;
            var message = new Byte[] { };
            var messageSize = message.Length + 1;
            var memoryStream = new MemoryStream();
            var binaryWriter = new BigEndianBinaryWriter(memoryStream);
            binaryWriter.Write(offset);
            binaryWriter.Write(messageSize);
            binaryWriter.Write(message);
            var payloadBytes = memoryStream.ToArray();

            // act/assert
            Assert.Throws<BufferUnderRunException>(() => Message.DecodeMessageSet(payloadBytes).ToList());
        }

        [Test]
        public void WhenMessageIsExactlyTheSizeOfBufferThenMessageIsDecoded()
        {
            // arrange
            var expectedPayloadBytes = new Byte[] { 1, 2, 3, 4 };
            var payload = MessageHelper.CreateMessage(0, new Byte[] { 0 }, expectedPayloadBytes);

            // act/assert
            var messages = Message.DecodeMessageSet(payload).ToList();
            var actualPayload = messages.First().Value;

            // assert
            var expectedPayload = new Byte[] { 1, 2, 3, 4 };
            CollectionAssert.AreEqual(expectedPayload, actualPayload);
        }
    }
}
