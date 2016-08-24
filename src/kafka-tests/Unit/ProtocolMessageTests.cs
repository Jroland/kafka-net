using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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

            data.Buffer.AssertProtocol(
                reader => {
                    reader.AssertRequestHeader(version, request);

                    Assert.That(reader.ReadInt16(), Is.EqualTo(request.Acks), "acks");
                    Assert.That(reader.ReadInt32(), Is.EqualTo(request.TimeoutMS), "timeout");

                    Assert.That(reader.ReadInt32(), Is.EqualTo(request.Payload.Count), "[topic_data]");
                    foreach (var payload in request.Payload) {
                        Assert.That(reader.ReadString(), Is.EqualTo(payload.Topic), "TopicName");
                        Assert.That(reader.ReadInt32(), Is.EqualTo(partitionsPerTopic), "[Partition]");
                        Assert.That(reader.ReadInt32(), Is.EqualTo(payload.Partition), "Partition");

                        var finalPosition = reader.ReadInt32() + reader.Position;
                        reader.AssertMessageSet(version, payload.Messages);
                        Assert.That(reader.Position, Is.EqualTo(finalPosition), "MessageSetSize");
                    }
                });
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
