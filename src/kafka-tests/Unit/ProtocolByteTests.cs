using System;
using System.Collections.Generic;
using System.IO;
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
    /// Message formats are from https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-CommonRequestandResponseStructure
    /// 
    /// RequestOrResponse => Size (RequestMessage | ResponseMessage)
    ///  Size => int32    : The Size field gives the size of the subsequent request or response message in bytes. 
    ///                     The client can read requests by first reading this 4 byte size as an integer N, and 
    ///                     then reading and parsing the subsequent N bytes of the request.
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
    public class ProtocolByteTests
    {
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
        [Test]
        public void ProduceApiRequest(
            [Values(0, 1, 2)] short version,
            [Values(0, 1, 2, -1)] short acks, 
            [Values(0, 1, 1000)] int timeoutMilliseconds, 
            [Values("test", "a really long name, with spaces and punctuation!")] string topic, 
            [Values(1, 10)] int topicsPerRequest, 
            [Values(1, 5)] int totalPartitions, 
            [Values(1, 2, 3)] int messagesPerSet)
        {
            var randomizer = new Randomizer();
            var clientId = nameof(ProduceApiRequest);

            var request = new ProduceRequest {
                Acks = acks,
                ClientId = clientId,
                CorrelationId = clientId.GetHashCode(),
                TimeoutMS = timeoutMilliseconds,
                Payload = new List<Payload>(),
                ApiVersion = version
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
                        Timestamp = DateTime.UtcNow,
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
                    reader.AssertRequestHeader(request);
                    reader.AssertProduceRequest(request);
                });
        }

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
        [Test]
        public void InterpretApiResponse(
            [Values(0, 1, 2)] short version,
            [Values(-1, 0, 123456, 10000000)] long timestampMilliseconds, 
            [Values("test", "a really long name, with spaces and punctuation!")] string topic, 
            [Values(1, 10)] int topicsPerRequest, 
            [Values(1, 5)] int totalPartitions, 
            [Values(
                ErrorResponseCode.NoError,
                ErrorResponseCode.InvalidMessage,
                ErrorResponseCode.NotCoordinatorForConsumerCode
            )] ErrorResponseCode errorCode,
            [Values(0, 1234, 100000)] int throttleTime)
        {
            var randomizer = new Randomizer();
            var clientId = nameof(InterpretApiResponse);
            var correlationId = clientId.GetHashCode();

            byte[] data = null;
            using (var stream = new MemoryStream()) {
                var writer = new BigEndianBinaryWriter(stream);
                writer.WriteResponseHeader(correlationId);

                writer.Write(topicsPerRequest);
                for (var t = 0; t < topicsPerRequest; t++) {
                    writer.Write(topic + t);
                    writer.Write(1); // partitionsPerTopic
                    writer.Write(t % totalPartitions);
                    writer.Write((short)errorCode);
                    writer.Write((long)randomizer.Next());
                    if (version >= 2) {
                        writer.Write(timestampMilliseconds);
                    }
                }
                if (version >= 1) {
                    writer.Write(throttleTime);    
                }
                data = new byte[stream.Position];
                Buffer.BlockCopy(stream.GetBuffer(), 0, data, 0, data.Length);
            }

            var request = new ProduceRequest { ApiVersion = version };
            var responses = request.Decode(data); // doesn't include the size in the decode -- the framework deals with it, I'd assume
            data.PrefixWithInt32Length().AssertProtocol(
                reader =>
                {
                    reader.AssertResponseHeader(correlationId);
                    reader.AssertProduceResponse(version, throttleTime, responses);
                });
        }

        /// <summary>
        /// FetchRequest => ReplicaId MaxWaitTime MinBytes [TopicName [Partition FetchOffset MaxBytes]]
        ///  ReplicaId => int32   -- The replica id indicates the node id of the replica initiating this request. Normal client consumers should always 
        ///                          specify this as -1 as they have no node id. Other brokers set this to be their own node id. The value -2 is accepted 
        ///                          to allow a non-broker to issue fetch requests as if it were a replica broker for debugging purposes.
        ///  MaxWaitTime => int32 -- The max wait time is the maximum amount of time in milliseconds to block waiting if insufficient data is available 
        ///                          at the time the request is issued.
        ///  MinBytes => int32    -- This is the minimum number of bytes of messages that must be available to give a response. If the client sets this 
        ///                          to 0 the server will always respond immediately, however if there is no new data since their last request they will 
        ///                          just get back empty message sets. If this is set to 1, the server will respond as soon as at least one partition has 
        ///                          at least 1 byte of data or the specified timeout occurs. By setting higher values in combination with the timeout the 
        ///                          consumer can tune for throughput and trade a little additional latency for reading only large chunks of data (e.g. 
        ///                          setting MaxWaitTime to 100 ms and setting MinBytes to 64k would allow the server to wait up to 100ms to try to accumulate 
        ///                          64k of data before responding).
        ///  TopicName => string  -- The name of the topic.
        ///  Partition => int32   -- The id of the partition the fetch is for.
        ///  FetchOffset => int64 -- The offset to begin this fetch from.
        ///  MaxBytes => int32    -- The maximum bytes to include in the message set for this partition. This helps bound the size of the response.
        /// 
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-FetchAPI
        /// </summary>
        [Test]
        public void FetchApiRequest(
            [Values(0, 1, 2)] short version,
            [Values(0, 10, 100)] int timeoutMilliseconds, 
            [Values(0, 64000)] int minBytes, 
            [Values("test", "a really long name, with spaces and punctuation!")] string topic, 
            [Values(1, 10)] int topicsPerRequest, 
            [Values(1, 5)] int totalPartitions, 
            [Values(64000, 25600000)] int maxBytes)
        {
            var randomizer = new Randomizer();
            var clientId = nameof(FetchApiRequest);

            var request = new FetchRequest {
                ClientId = clientId,
                CorrelationId = clientId.GetHashCode(),
                Fetches = new List<Fetch>(),
                ApiVersion = version
            };

            for (var t = 0; t < topicsPerRequest; t++) {
                var payload = new Fetch {
                    Topic = topic + t,
                    PartitionId = t % totalPartitions,
                    Offset = randomizer.Next(0, int.MaxValue),
                    MaxBytes = maxBytes
                };
                request.Fetches.Add(payload);
            }

            var data = request.Encode();

            data.Buffer.AssertProtocol(
                reader => {
                    reader.AssertRequestHeader(request);
                    reader.AssertFetchRequest(request);
                });
        }

        public static IEnumerable<ErrorResponseCode> ErrorResponseCodes => new[] {
            ErrorResponseCode.NoError,
            ErrorResponseCode.Unknown,
            ErrorResponseCode.OffsetOutOfRange,
            ErrorResponseCode.InvalidMessage,
            ErrorResponseCode.UnknownTopicOrPartition,
            ErrorResponseCode.InvalidMessageSize,
            ErrorResponseCode.LeaderNotAvailable,
            ErrorResponseCode.NotLeaderForPartition,
            ErrorResponseCode.RequestTimedOut,
            ErrorResponseCode.BrokerNotAvailable,
            ErrorResponseCode.ReplicaNotAvailable,
            ErrorResponseCode.MessageSizeTooLarge,
            //ErrorResponseCode.StaleControllerEpochCode, 
            ErrorResponseCode.OffsetMetadataTooLargeCode,
            ErrorResponseCode.OffsetsLoadInProgressCode,
            ErrorResponseCode.ConsumerCoordinatorNotAvailableCode,
            ErrorResponseCode.NotCoordinatorForConsumerCode
        };
    }
}