using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using KafkaNet;
using KafkaNet.Common;
using KafkaNet.Protocol;
using NUnit.Framework;

namespace kafka_tests.Unit
{
    public static class ProtocolAssertionExtensions
    {
        private static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        /// <summary>
        /// ProduceResponse => [TopicName [Partition ErrorCode Offset *Timestamp]] *ThrottleTime
        ///  *ThrottleTime is only version 1 (0.9.0) and above
        ///  *Timestamp is only version 2 (0.10.0) and above
        ///  TopicName => string   -- The topic this response entry corresponds to.
        ///  Partition => int32    -- The partition this response entry corresponds to.
        ///  ErrorCode => int16    -- The error from this partition, if any. Errors are given on a per-partition basis because a given partition may be 
        ///                           unavailable or maintained on a different host, while others may have successfully accepted the produce request.
        ///  Offset => int64       -- The offset assigned to the first message in the message set appended to this partition.
        ///  Timestamp => int64    -- If LogAppendTime is used for the topic, this is the timestamp assigned by the broker to the message set. 
        ///                           All the messages in the message set have the same timestamp.
        ///                           If CreateTime is used, this field is always -1. The producer can assume the timestamp of the messages in the 
        ///                           produce request has been accepted by the broker if there is no error code returned.
        ///                           Unit is milliseconds since beginning of the epoch (midnight Jan 1, 1970 (UTC)).
        ///  ThrottleTime => int32 -- Duration in milliseconds for which the request was throttled due to quota violation. 
        ///                           (Zero if the request did not violate any quota).
        /// 
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets
        /// </summary>
        public static void AssertProduceResponse(this BigEndianBinaryReader reader, int version, int throttleTime, IEnumerable<ProduceResponse> response)
        {
            var responses = response.ToList();
            Assert.That(reader.ReadInt32(), Is.EqualTo(responses.Count), "[TopicName]");
            foreach (var payload in responses) {
                Assert.That(reader.ReadString(), Is.EqualTo(payload.Topic), "TopicName");
                Assert.That(reader.ReadInt32(), Is.EqualTo(1), "[Partition]"); // this is a mismatch between the protocol and the object model
                Assert.That(reader.ReadInt32(), Is.EqualTo(payload.PartitionId), "Partition");
                Assert.That(reader.ReadInt16(), Is.EqualTo(payload.Error), "Error");
                Assert.That(reader.ReadInt64(), Is.EqualTo(payload.Offset), "Offset");
                if (version >= 2) {
                    var timestamp = reader.ReadInt64();
                    if (timestamp == -1L) {
                        Assert.That(payload.Timestamp, Is.Null, "Timestamp");
                    } else {
                        Assert.That(payload.Timestamp, Is.Not.Null, "Timestamp");
                        Assert.That(payload.Timestamp.Value, Is.EqualTo(UnixEpoch.AddMilliseconds(timestamp)), "Timestamp");
                    }
                }
            }
            if (version >= 1) {
                Assert.That(reader.ReadInt32(), Is.EqualTo(throttleTime), "ThrottleTime");
            }
        }

        /// <summary>
        /// ProduceRequest => RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]
        ///  RequiredAcks => int16   -- This field indicates how many acknowledgements the servers should receive before responding to the request. 
        ///                             If it is 0 the server will not send any response (this is the only case where the server will not reply to 
        ///                             a request). If it is 1, the server will wait the data is written to the local log before sending a response. 
        ///                             If it is -1 the server will block until the message is committed by all in sync replicas before sending a response.
        ///  Timeout => int32        -- This provides a maximum time in milliseconds the server can await the receipt of the number of acknowledgements 
        ///                             in RequiredAcks. The timeout is not an exact limit on the request time for a few reasons: (1) it does not include 
        ///                             network latency, (2) the timer begins at the beginning of the processing of this request so if many requests are 
        ///                             queued due to server overload that wait time will not be included, (3) we will not terminate a local write so if 
        ///                             the local write time exceeds this timeout it will not be respected. To get a hard timeout of this type the client 
        ///                             should use the socket timeout.
        ///  TopicName => string     -- The topic that data is being published to.
        ///  Partition => int32      -- The partition that data is being published to.
        ///  MessageSetSize => int32 -- The size, in bytes, of the message set that follows.
        /// 
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets
        /// </summary>
        public static void AssertProduceRequest(this BigEndianBinaryReader reader, ProduceRequest request)
        {
            Assert.That(reader.ReadInt16(), Is.EqualTo(request.Acks), "acks");
            Assert.That(reader.ReadInt32(), Is.EqualTo(request.TimeoutMS), "timeout");

            Assert.That(reader.ReadInt32(), Is.EqualTo(request.Payload.Count), "[topic_data]");
            foreach (var payload in request.Payload) {
                Assert.That(reader.ReadString(), Is.EqualTo(payload.Topic), "TopicName");
                Assert.That(reader.ReadInt32(), Is.EqualTo(1), "[Partition]"); // this is a mismatch between the protocol and the object model
                Assert.That(reader.ReadInt32(), Is.EqualTo(payload.Partition), "Partition");

                var finalPosition = reader.ReadInt32() + reader.Position;
                reader.AssertMessageSet(request.ApiVersion, payload.Messages);
                Assert.That(reader.Position, Is.EqualTo(finalPosition), $"MessageSetSize was {finalPosition - 4} but ended in a different spot.");
            }
        }

        /// <summary>
        /// MessageSet => [Offset MessageSize Message]
        ///  Offset => int64      -- This is the offset used in kafka as the log sequence number. When the producer is sending non compressed messages, 
        ///                          it can set the offsets to anything. When the producer is sending compressed messages, to avoid server side recompression, 
        ///                          each compressed message should have offset starting from 0 and increasing by one for each inner message in the compressed message. 
        ///  MessageSize => int32
        /// 
        /// NB. MessageSets are not preceded by an int32 like other array elements in the protocol:
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets
        /// </summary>
        public static void AssertMessageSet(this BigEndianBinaryReader reader, int version, IEnumerable<Message> messages)
        {
            foreach (var message in messages) {
                var offset = reader.ReadInt64();
                if (message.Attribute != (byte)MessageCodec.CodecNone) {
                    // TODO: assert offset?
                }
                var finalPosition = reader.ReadInt32() + reader.Position;
                reader.AssertMessage(version, message);
                Assert.That(reader.Position, Is.EqualTo(finalPosition), $"MessageSize was {finalPosition - 4} but ended in a different spot.");
            }
        }

        /// <summary>
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets
        /// 
        /// Message => Crc MagicByte Attributes *Timestamp Key Value
        ///  *Timestamp is only version 2 (0.10) and above
        ///   Crc => int32       -- The CRC is the CRC32 of the remainder of the message bytes. This is used to check the integrity of the message on the broker and consumer.
        ///   MagicByte => int8  -- This is a version id used to allow backwards compatible evolution of the message binary format. The current value is 1.
        ///   Attributes => int8 -- This byte holds metadata attributes about the message.
        ///                         The lowest 3 bits contain the compression codec used for the message.
        ///                         The fourth lowest bit represents the timestamp type. 0 stands for CreateTime and 1 stands for LogAppendTime. The producer should always set this bit to 0. (since 0.10.0)
        ///                         All other bits should be set to 0.
        ///   Timestamp => int64 -- This is the timestamp of the message. The timestamp type is indicated in the attributes. Unit is milliseconds since beginning of the epoch (midnight Jan 1, 1970 (UTC)).
        ///   Key => bytes       -- The key is an optional message key that was used for partition assignment. The key can be null.
        ///   Value => bytes     -- The value is the actual message contents as an opaque byte array. Kafka supports recursive messages in which case this may itself contain a message set. The message can be null.
        /// </summary>
        public static void AssertMessage(this BigEndianBinaryReader reader, int version, Message message)
        {
            var crc = (uint)reader.ReadInt32();
            var positionAfterCrc = reader.Position;
            Assert.That(reader.ReadByte(), Is.EqualTo(message.MagicNumber), "MagicByte");
            Assert.That(reader.ReadByte(), Is.EqualTo(message.Attribute), "Attributes");
            if (version == 2) {
                Assert.That(reader.ReadInt64(), Is.EqualTo((long)(message.Timestamp - UnixEpoch).TotalMilliseconds), "Timestamp");
            }
            Assert.That(reader.ReadBytes(), Is.EqualTo(message.Key), "Key");
            Assert.That(reader.ReadBytes(), Is.EqualTo(message.Value), "Value");

            var positionAfterMessage = reader.Position;
            reader.Position = positionAfterCrc;
            var crcBytes = reader.ReadBytes((int) (positionAfterMessage - positionAfterCrc));
            Assert.That(Crc32Provider.Compute(crcBytes), Is.EqualTo(crc));
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
        public static void AssertRequestHeader<T>(this BigEndianBinaryReader reader, IKafkaRequest<T> request)
        {
            reader.AssertRequestHeader(request.ApiKey, request.ApiVersion, request.CorrelationId, request.ClientId);
        }

        /// <summary>
        /// MessageSet => [Offset MessageSize Message]
        ///  Offset => int64      -- This is the offset used in kafka as the log sequence number. When the producer is sending non compressed messages, 
        ///                          it can set the offsets to anything. When the producer is sending compressed messages, to avoid server side recompression, 
        ///                          each compressed message should have offset starting from 0 and increasing by one for each inner message in the compressed message. 
        ///  MessageSize => int32
        /// 
        /// NB. MessageSets are not preceded by an int32 like other array elements in the protocol:
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets
        /// </summary>
        public static void AssertMessageSet(this BigEndianBinaryReader reader, int version, IEnumerable<Tuple<byte, DateTime, byte[], byte[]>> messageValues)
        {
            var messages = messageValues.Select(
                t =>
                new Message {
                    Attribute = t.Item1,
                    Timestamp = t.Item2,
                    Key = t.Item3,
                    Value = t.Item4,
                    MagicNumber = 1
                });
            reader.AssertMessageSet(version, messages);
        }

        /// <summary>
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets
        /// 
        /// version 0:
        /// Message => Crc MagicByte Attributes Key Value
        /// version 1:
        /// Message => Crc MagicByte Attributes Timestamp Key Value
        ///   Crc => int32       -- The CRC is the CRC32 of the remainder of the message bytes. This is used to check the integrity of the message on the broker and consumer.
        ///   MagicByte => int8  -- This is a version id used to allow backwards compatible evolution of the message binary format. The current value is 1.
        ///   Attributes => int8 -- This byte holds metadata attributes about the message.
        ///                         The lowest 3 bits contain the compression codec used for the message.
        ///                         The fourth lowest bit represents the timestamp type. 0 stands for CreateTime and 1 stands for LogAppendTime. The producer should always set this bit to 0. (since 0.10.0)
        ///                         All other bits should be set to 0.
        ///   Timestamp => int64 -- This is the timestamp of the message. The timestamp type is indicated in the attributes. Unit is milliseconds since beginning of the epoch (midnight Jan 1, 1970 (UTC)).
        ///   Key => bytes       -- The key is an optional message key that was used for partition assignment. The key can be null.
        ///   Value => bytes     -- The value is the actual message contents as an opaque byte array. Kafka supports recursive messages in which case this may itself contain a message set. The message can be null.
        /// </summary>
        public static void AssertMessage(this BigEndianBinaryReader reader, int version, byte attributes, DateTime timestamp, byte[] key, byte[] value)
        {
            reader.AssertMessage(version, new Message { Attribute = attributes, Timestamp = timestamp, Key = key, Value = value, MagicNumber = 1});
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
        public static void AssertRequestHeader(this BigEndianBinaryReader reader, ApiKeyRequestType apiKey, short version, int correlationId, string clientId)
        {
            Assert.That(reader.ReadInt16(), Is.EqualTo((short) apiKey), "api_key");
            Assert.That(reader.ReadInt16(), Is.EqualTo(version), "api_version");
            Assert.That(reader.ReadInt32(), Is.EqualTo(correlationId), "correlation_id");
            Assert.That(reader.ReadString(), Is.EqualTo(clientId), "client_id");
        }

        /// <summary>
        /// From http://kafka.apache.org/protocol.html#protocol_messages
        /// 
        /// Response Header => correlation_id 
        ///  correlation_id => INT32  -- The user-supplied value passed in with the request
        /// </summary>
        public static void AssertResponseHeader(this BigEndianBinaryReader reader, int correlationId)
        {
            Assert.That(reader.ReadInt32(), Is.EqualTo(correlationId));
        }

        /// <summary>
        /// RequestOrResponse => Size (RequestMessage | ResponseMessage)
        ///  Size => int32    -- The Size field gives the size of the subsequent request or response message in bytes. 
        ///                      The client can read requests by first reading this 4 byte size as an integer N, and 
        ///                      then reading and parsing the subsequent N bytes of the request.
        /// 
        /// From: https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-CommonRequestandResponseStructure
        /// </summary>
        public static void AssertProtocol(this byte[] bytes, Action<BigEndianBinaryReader> assertions)
        {
            using (var reader = new BigEndianBinaryReader(bytes)) {
                var finalPosition = reader.ReadInt32() + reader.Position;
                assertions(reader);
                Assert.That(finalPosition, Is.EqualTo(reader.Position), $"Size was {finalPosition - 4} but ended in a different spot.");
            }
        }

        public static byte[] PrefixWithInt32Length(this byte[] source)
        {
            var destination = new byte[source.Length + 4]; 
            using (var stream = new MemoryStream(destination)) {
                using (var writer = new BigEndianBinaryWriter(stream)) {
                    writer.Write(source.Length);
                }
            }
            Buffer.BlockCopy(source, 0, destination, 4, source.Length);
            return destination;
        }

        /// <summary>
        /// From http://kafka.apache.org/protocol.html#protocol_messages
        /// 
        /// Response Header => correlation_id 
        ///  correlation_id => INT32  -- The user-supplied value passed in with the request
        /// </summary>
        public static void WriteResponseHeader(this BigEndianBinaryWriter writer, int correlationId)
        {
            writer.Write(correlationId);
        }

    }
}