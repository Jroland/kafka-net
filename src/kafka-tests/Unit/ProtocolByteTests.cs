using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
            [Values(0, 2, -1)] short acks, 
            [Values(0, 1000)] int timeoutMilliseconds, 
            [Values("test", "a really long name, with spaces and punctuation!")] string topic, 
            [Values(1, 10)] int topicsPerRequest, 
            [Values(1, 5)] int totalPartitions, 
            [Values(3)] int messagesPerSet)
        {
            var clientId = "ProduceApiRequest";

            var request = new ProduceRequest {
                Acks = acks,
                ClientId = clientId,
                CorrelationId = clientId.GetHashCode(),
                TimeoutMS = timeoutMilliseconds,
                Payload = new List<Payload>(),
                ApiVersion = version
            };

            for (var t = 0; t < topicsPerRequest; t++) {
                request.Payload.Add(
                    new Payload {
                        Topic = topic + t,
                        Partition = t % totalPartitions,
                        Codec = MessageCodec.CodecNone,
                        Messages = GenerateMessages(messagesPerSet, (byte) (version >= 2 ? 1 : 0))
                    });
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
        public void ProduceApiResponse(
            [Values(0, 1, 2)] short version,
            [Values(-1, 0, 10000000)] long timestampMilliseconds, 
            [Values("test", "a really long name, with spaces and punctuation!")] string topic, 
            [Values(1, 10)] int topicsPerRequest, 
            [Values(1, 5)] int totalPartitions, 
            [Values(
                ErrorResponseCode.NoError,
                ErrorResponseCode.InvalidMessage
            )] ErrorResponseCode errorCode,
            [Values(0, 100000)] int throttleTime)
        {
            var randomizer = new Randomizer();
            var clientId = "ProduceApiResponse";
            var correlationId = clientId.GetHashCode();

            byte[] data = null;
            using (var stream = new MemoryStream()) {
                var writer = new BigEndianBinaryWriter(stream);
                writer.WriteResponseHeader(correlationId);

                writer.Write(topicsPerRequest);
                for (var t = 0; t < topicsPerRequest; t++) {
                    writer.Write(topic + t, StringPrefixEncoding.Int16);
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
            [Values(0, 100)] int timeoutMilliseconds, 
            [Values(0, 64000)] int minBytes, 
            [Values("test", "a really long name, with spaces and punctuation!")] string topic, 
            [Values(1, 10)] int topicsPerRequest, 
            [Values(1, 5)] int totalPartitions, 
            [Values(25600000)] int maxBytes)
        {
            var randomizer = new Randomizer();
            var clientId = "FetchApiRequest";

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

        /// <summary>
        /// FetchResponse => *ThrottleTime [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
        ///  *ThrottleTime is only version 1 (0.9.0) and above
        ///  ThrottleTime => int32        -- Duration in milliseconds for which the request was throttled due to quota violation. (Zero if the request did not 
        ///                                  violate any quota.)
        ///  TopicName => string          -- The topic this response entry corresponds to.
        ///  Partition => int32           -- The partition this response entry corresponds to.
        ///  ErrorCode => int16           -- The error from this partition, if any. Errors are given on a per-partition basis because a given partition may 
        ///                                  be unavailable or maintained on a different host, while others may have successfully accepted the produce request.
        ///  HighwaterMarkOffset => int64 -- The offset at the end of the log for this partition. This can be used by the client to determine how many messages 
        ///                                  behind the end of the log they are.
        ///  MessageSetSize => int32      -- The size in bytes of the message set for this partition
        /// 
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-FetchResponse
        /// </summary>
        [Test]
        public void FetchApiResponse(
            [Values(0, 1, 2)] short version,
            [Values(0, 1234)] int throttleTime,
            [Values("test", "a really long name, with spaces and punctuation!")] string topic, 
            [Values(1, 10)] int topicsPerRequest, 
            [Values(1, 5)] int totalPartitions, 
            [Values(
                ErrorResponseCode.NoError,
                ErrorResponseCode.OffsetOutOfRange
            )] ErrorResponseCode errorCode, 
            [Values(3)] int messagesPerSet
            )
        {
            var randomizer = new Randomizer();
            var clientId = "FetchApiResponse";
            var correlationId = clientId.GetHashCode();

            byte[] data = null;
            using (var stream = new MemoryStream()) {
                var writer = new BigEndianBinaryWriter(stream);
                writer.WriteResponseHeader(correlationId);

                if (version >= 1) {
                    writer.Write(throttleTime);
                }
                writer.Write(topicsPerRequest);
                for (var t = 0; t < topicsPerRequest; t++) {
                    writer.Write(topic + t, StringPrefixEncoding.Int16);
                    writer.Write(1); // partitionsPerTopic
                    writer.Write(t % totalPartitions);
                    writer.Write((short)errorCode);
                    writer.Write((long)randomizer.Next());

                    var messageSet = Message.EncodeMessageSet(GenerateMessages(messagesPerSet, (byte) (version >= 2 ? 1 : 0)));
                    writer.Write(messageSet.Length);
                    writer.Write(messageSet);
                }
                data = new byte[stream.Position];
                Buffer.BlockCopy(stream.GetBuffer(), 0, data, 0, data.Length);
            }

            var request = new FetchRequest { ApiVersion = version };
            var responses = request.Decode(data); // doesn't include the size in the decode -- the framework deals with it, I'd assume
            data.PrefixWithInt32Length().AssertProtocol(
                reader =>
                {
                    reader.AssertResponseHeader(correlationId);
                    reader.AssertFetchResponse(version, throttleTime, responses);
                });
        }

        /// <summary>
        /// OffsetRequest => ReplicaId [TopicName [Partition Time MaxNumberOfOffsets]]
        ///  ReplicaId => int32   -- The replica id indicates the node id of the replica initiating this request. Normal client consumers should always 
        ///                          specify this as -1 as they have no node id. Other brokers set this to be their own node id. The value -2 is accepted 
        ///                          to allow a non-broker to issue fetch requests as if it were a replica broker for debugging purposes.
        ///  TopicName => string  -- The name of the topic.
        ///  Partition => int32   -- The id of the partition the fetch is for.
        ///  Time => int64        -- Used to ask for all messages before a certain time (ms). There are two special values. Specify -1 to receive the 
        ///                          latest offset (i.e. the offset of the next coming message) and -2 to receive the earliest available offset. Note 
        ///                          that because offsets are pulled in descending order, asking for the earliest offset will always return you a single element.
        ///  MaxNumberOfOffsets => int32 
        /// 
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetAPI(AKAListOffset)
        /// </summary>
        [Test]
        public void OffsetsApiRequest(
            [Values("test", "a really long name, with spaces and punctuation!")] string topic, 
            [Values(1, 10)] int topicsPerRequest, 
            [Values(1, 5)] int totalPartitions, 
            [Values(-2, -1, 123456, 10000000)] long time,
            [Values(1, 10)] int maxOffsets)
        {
            var clientId = "OffsetsApiRequest";

            var request = new OffsetRequest {
                ClientId = clientId,
                CorrelationId = clientId.GetHashCode(),
                Offsets = new List<Offset>(),
                ApiVersion = 0
            };

            for (var t = 0; t < topicsPerRequest; t++) {
                var payload = new Offset {
                    Topic = topic + t,
                    PartitionId = t % totalPartitions,
                    Time = time,
                    MaxOffsets = maxOffsets
                };
                request.Offsets.Add(payload);
            }

            var data = request.Encode();

            data.Buffer.AssertProtocol(
                reader => {
                    reader.AssertRequestHeader(request);
                    reader.AssertOffsetRequest(request);
                });
        }

        /// <summary>
        /// OffsetResponse => [TopicName [PartitionOffsets]]
        ///  PartitionOffsets => Partition ErrorCode [Offset]
        ///  TopicName => string  -- The name of the topic.
        ///  Partition => int32   -- The id of the partition the fetch is for.
        ///  ErrorCode => int16   -- The error from this partition, if any. Errors are given on a per-partition basis because a given partition may 
        ///                          be unavailable or maintained on a different host, while others may have successfully accepted the produce request.
        ///  Offset => int64
        /// 
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetAPI(AKAListOffset)
        /// </summary>
        [Test]
        public void OffsetsApiResponse(
            [Values("test", "a really long name, with spaces and punctuation!")] string topic, 
            [Values(1, 10)] int topicsPerRequest, 
            [Values(5)] int totalPartitions, 
            [Values(
                ErrorResponseCode.UnknownTopicOrPartition,
                ErrorResponseCode.NotLeaderForPartition,
                ErrorResponseCode.Unknown
            )] ErrorResponseCode errorCode, 
            [Values(1, 5)] int offsetsPerPartition)
        {
            var randomizer = new Randomizer();
            var clientId = "OffsetsApiResponse";
            var correlationId = clientId.GetHashCode();

            byte[] data = null;
            using (var stream = new MemoryStream()) {
                var writer = new BigEndianBinaryWriter(stream);
                writer.WriteResponseHeader(correlationId);

                writer.Write(topicsPerRequest);
                for (var t = 0; t < topicsPerRequest; t++) {
                    writer.Write(topic + t, StringPrefixEncoding.Int16);
                    writer.Write(1); // partitionsPerTopic
                    writer.Write(t % totalPartitions);
                    writer.Write((short)errorCode);
                    writer.Write(offsetsPerPartition);
                    for (var o = 0; o < offsetsPerPartition; o++) {
                        writer.Write((long)randomizer.Next());
                    }
                }
                data = new byte[stream.Position];
                Buffer.BlockCopy(stream.GetBuffer(), 0, data, 0, data.Length);
            }

            var request = new OffsetRequest { ApiVersion = 0 };
            var responses = request.Decode(data); // doesn't include the size in the decode -- the framework deals with it, I'd assume
            data.PrefixWithInt32Length().AssertProtocol(
                reader =>
                {
                    reader.AssertResponseHeader(correlationId);
                    reader.AssertOffsetResponse(responses);
                });
        }

        /// <summary>
        /// TopicMetadataRequest => [TopicName]
        ///  TopicName => string  -- The topics to produce metadata for. If no topics are specified fetch metadata for all topics.
        ///
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-MetadataAPI
        /// </summary>
        [Test]
        public void MetadataApiRequest(
            [Values("test", "a really long name, with spaces and punctuation!")] string topic,
            [Values(0, 1, 10)] int topicsPerRequest)
        {
            var clientId = "MetadataApiRequest";

            var request = new MetadataRequest {
                ClientId = clientId,
                CorrelationId = clientId.GetHashCode(),
                Topics = new List<string>(),
                ApiVersion = 0
            };

            for (var t = 0; t < topicsPerRequest; t++) {
                request.Topics.Add(topic + t);
            }

            var data = request.Encode();

            data.Buffer.AssertProtocol(
                     reader => {
                         reader.AssertRequestHeader(request);
                         reader.AssertMetadataRequest(request);
                     });
        }

        /// <summary>
        /// MetadataResponse => [Broker][TopicMetadata]
        ///  Broker => NodeId Host Port  (any number of brokers may be returned)
        ///                               -- The node id, hostname, and port information for a kafka broker
        ///   NodeId => int32             -- The broker id.
        ///   Host => string              -- The hostname of the broker.
        ///   Port => int32               -- The port on which the broker accepts requests.
        ///  TopicMetadata => TopicErrorCode TopicName [PartitionMetadata]
        ///   TopicErrorCode => int16     -- The error code for the given topic.
        ///   TopicName => string         -- The name of the topic.
        ///  PartitionMetadata => PartitionErrorCode PartitionId Leader Replicas Isr
        ///   PartitionErrorCode => int16 -- The error code for the partition, if any.
        ///   PartitionId => int32        -- The id of the partition.
        ///   Leader => int32             -- The id of the broker acting as leader for this partition.
        ///                                  If no leader exists because we are in the middle of a leader election this id will be -1.
        ///   Replicas => [int32]         -- The set of all nodes that host this partition.
        ///   Isr => [int32]              -- The set of nodes that are in sync with the leader for this partition.
        ///
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-MetadataAPI
        /// </summary>
        [Test]
        public void MetadataApiResponse(
            [Values(1, 5)] int brokersPerRequest,
            [Values("test", "a really long name, with spaces and punctuation!")] string topic,
            [Values(1, 10)] int topicsPerRequest,
            [Values(1, 5)] int partitionsPerTopic,
            [Values(
                 ErrorResponseCode.NoError,
                 ErrorResponseCode.UnknownTopicOrPartition
             )] ErrorResponseCode errorCode)
        {
            var randomizer = new Randomizer();
            var clientId = "MetadataApiResponse";
            var correlationId = clientId.GetHashCode();

            byte[] data = null;
            using (var stream = new MemoryStream()) {
                var writer = new BigEndianBinaryWriter(stream);
                writer.WriteResponseHeader(correlationId);

                writer.Write(brokersPerRequest);
                for (var b = 0; b < brokersPerRequest; b++) {
                    writer.Write(b);
                    writer.Write("broker-" + b, StringPrefixEncoding.Int16);
                    writer.Write(9092 + b);
                }

                writer.Write(topicsPerRequest);
                for (var t = 0; t < topicsPerRequest; t++) {
                    writer.Write((short) errorCode);
                    writer.Write(topic + t, StringPrefixEncoding.Int16);
                    writer.Write(partitionsPerTopic);
                    for (var p = 0; p < partitionsPerTopic; p++) {
                        writer.Write((short) errorCode);
                        writer.Write(p);
                        var leader = randomizer.Next(0, brokersPerRequest - 1);
                        writer.Write(leader);
                        var replicas = randomizer.Next(0, brokersPerRequest - 1);
                        writer.Write(replicas);
                        for (var r = 0; r < replicas; r++) {
                            writer.Write(r);
                        }
                        var isr = randomizer.Next(0, replicas);
                        writer.Write(isr);
                        for (var i = 0; i < isr; i++) {
                            writer.Write(i);
                        }
                    }
                }
                data = new byte[stream.Position];
                Buffer.BlockCopy(stream.GetBuffer(), 0, data, 0, data.Length);
            }

            var request = new MetadataRequest {ApiVersion = 0};
            var responses = request.Decode(data).Single(); // note that this is a bit weird
                // doesn't include the size in the decode -- the framework deals with it, I'd assume
            data.PrefixWithInt32Length().AssertProtocol(
                     reader => {
                         reader.AssertResponseHeader(correlationId);
                         reader.AssertMetadataResponse(responses);
                     });
        }

        /// <summary>
        /// OffsetCommitRequest => ConsumerGroup *ConsumerGroupGenerationId *MemberId *RetentionTime [TopicName [Partition Offset *TimeStamp Metadata]]
        /// *ConsumerGroupGenerationId, MemberId is only version 1 (0.8.2) and above
        /// *TimeStamp is only version 1 (0.8.2)
        /// *RetentionTime is only version 2 (0.9.0) and above
        ///  ConsumerGroupId => string          -- The consumer group id.
        ///  ConsumerGroupGenerationId => int32 -- The generation of the consumer group.
        ///  MemberId => string                 -- The consumer id assigned by the group coordinator.
        ///  RetentionTime => int64             -- Time period in ms to retain the offset.
        ///  TopicName => string                -- The topic to commit.
        ///  Partition => int32                 -- The partition id.
        ///  Offset => int64                    -- message offset to be committed.
        ///  Timestamp => int64                 -- Commit timestamp.
        ///  Metadata => string                 -- Any associated metadata the client wants to keep
        ///
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
        /// </summary>
        [Test]
        public void OffsetCommitApiRequest(
            [Values(0, 1, 2)] short version,
            [Values("group1", "group2")] string groupId,
            [Values(0, 5)] int generation,
            [Values(-1, 20000)] int retentionTime,
            [Values("test", "a really long name, with spaces and punctuation!")] string topic,
            [Values(1, 10)] int topicsPerRequest,
            [Values(5)] int maxPartitions,
            [Values(10)] int maxOffsets,
            [Values(null, "something useful for the client")] string metadata)
        {
            var clientId = "OffsetCommitApiRequest";
            var randomizer = new Randomizer();

            var request = new OffsetCommitRequest {
                ClientId = clientId,
                CorrelationId = clientId.GetHashCode(),
                ConsumerGroup = groupId,
                OffsetCommits = new List<OffsetCommit>(),
                ApiVersion = version,
                GenerationId = generation,
                MemberId = "member" + generation
            };

            if (retentionTime >= 0) {
                request.OffsetRetention = TimeSpan.FromMilliseconds(retentionTime);
            }
            for (var t = 0; t < topicsPerRequest; t++) {
                var payload = new OffsetCommit {
                    Topic = topic + t,
                    PartitionId = t % maxPartitions,
                    Offset = randomizer.Next(0, int.MaxValue),
                    Metadata = metadata
                };
                payload.TimeStamp = retentionTime;
                request.OffsetCommits.Add(payload);
            }

            var data = request.Encode();

            data.Buffer.AssertProtocol(
                     reader =>
                     {
                         reader.AssertRequestHeader(request);
                         reader.AssertOffsetCommitRequest(request);
                     });
        }

        /// <summary>
        /// OffsetCommitResponse => [TopicName [Partition ErrorCode]]]
        ///  TopicName => string -- The name of the topic.
        ///  Partition => int32  -- The id of the partition.
        ///  ErrorCode => int16  -- The error code for the partition, if any.
        ///
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
        /// </summary>
        [Test]
        public void OffsetCommitApiResponse(
            [Values("test", "a really long name, with spaces and punctuation!")] string topic,
            [Values(1, 10)] int topicsPerRequest,
            [Values(1, 5)] int partitionsPerTopic,
            [Values(
                 ErrorResponseCode.NoError,
                 ErrorResponseCode.OffsetMetadataTooLargeCode
             )] ErrorResponseCode errorCode)
        {
            var clientId = "OffsetCommitApiResponse";
            var correlationId = clientId.GetHashCode();

            byte[] data = null;
            using (var stream = new MemoryStream()) {
                var writer = new BigEndianBinaryWriter(stream);
                writer.WriteResponseHeader(correlationId);

                writer.Write(topicsPerRequest);
                for (var t = 0; t < topicsPerRequest; t++) {
                    writer.Write(topic + t, StringPrefixEncoding.Int16);
                    writer.Write(partitionsPerTopic);
                    for (var p = 0; p < partitionsPerTopic; p++) {
                        writer.Write(p);
                        writer.Write((short) errorCode);
                    }
                }
                data = new byte[stream.Position];
                Buffer.BlockCopy(stream.GetBuffer(), 0, data, 0, data.Length);
            }

            var request = new OffsetCommitRequest {ApiVersion = 0};
            var responses = request.Decode(data); 
                // doesn't include the size in the decode -- the framework deals with it, I'd assume
            data.PrefixWithInt32Length().AssertProtocol(
                     reader => {
                         reader.AssertResponseHeader(correlationId);
                         reader.AssertOffsetCommitResponse(responses);
                     });
        }

        /// <summary>
        /// OffsetFetchRequest => ConsumerGroup [TopicName [Partition]]
        ///  ConsumerGroup => string -- The consumer group id.
        ///  TopicName => string     -- The topic to commit.
        ///  Partition => int32      -- The partition id.
        ///
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
        /// </summary>
        [Test]
        public void OffsetFetchApiRequest(
            [Values("group1", "group2")] string groupId,
            [Values("test", "a really long name, with spaces and punctuation!")] string topic,
            [Values(1, 10)] int topicsPerRequest,
            [Values(5)] int maxPartitions)
        {
            var clientId = "OffsetFetchApiRequest";

            var request = new OffsetFetchRequest {
                ClientId = clientId,
                CorrelationId = clientId.GetHashCode(),
                ConsumerGroup = groupId,
                Topics = new List<OffsetFetch>(),
                ApiVersion = 0
            };

            for (var t = 0; t < topicsPerRequest; t++) {
                var payload = new OffsetFetch {
                    Topic = topic + t,
                    PartitionId = t % maxPartitions
                };
                request.Topics.Add(payload);
            }

            var data = request.Encode();

            data.Buffer.AssertProtocol(
                     reader =>
                     {
                         reader.AssertRequestHeader(request);
                         reader.AssertOffsetFetchRequest(request);
                     });
        }

        /// <summary>
        /// OffsetFetchResponse => [TopicName [Partition Offset Metadata ErrorCode]]
        ///  TopicName => string -- The name of the topic.
        ///  Partition => int32  -- The id of the partition.
        ///  Offset => int64     -- The offset, or -1 if none exists.
        ///  Metadata => string  -- The metadata associated with the topic and partition.
        ///  ErrorCode => int16  -- The error code for the partition, if any.
        ///
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
        /// </summary>
        [Test]
        public void OffsetFetchApiResponse(
            [Values("test", "a really long name, with spaces and punctuation!")] string topic,
            [Values(1, 10)] int topicsPerRequest,
            [Values(1, 5)] int partitionsPerTopic,
            [Values(
                 ErrorResponseCode.NoError,
                 ErrorResponseCode.UnknownTopicOrPartition,
                 ErrorResponseCode.OffsetsLoadInProgressCode,
                 ErrorResponseCode.NotCoordinatorForConsumerCode,
                 ErrorResponseCode.IllegalGeneration,
                 ErrorResponseCode.UnknownMemberId,
                 ErrorResponseCode.TopicAuthorizationFailed,
                 ErrorResponseCode.GroupAuthorizationFailed
             )] ErrorResponseCode errorCode)
        {
            var clientId = "OffsetFetchApiResponse";
            var correlationId = clientId.GetHashCode();
            var randomizer = new Randomizer();

            byte[] data = null;
            using (var stream = new MemoryStream()) {
                var writer = new BigEndianBinaryWriter(stream);
                writer.WriteResponseHeader(correlationId);

                writer.Write(topicsPerRequest);
                for (var t = 0; t < topicsPerRequest; t++) {
                    writer.Write(topic + t, StringPrefixEncoding.Int16);
                    writer.Write(partitionsPerTopic);
                    for (var p = 0; p < partitionsPerTopic; p++) {
                        writer.Write(p);
                        var offset = (long)randomizer.Next(int.MinValue, int.MaxValue);
                        writer.Write(offset);
                        writer.Write(offset >= 0 ? topic : string.Empty, StringPrefixEncoding.Int16);
                        writer.Write((short) errorCode);
                    }
                }
                data = new byte[stream.Position];
                Buffer.BlockCopy(stream.GetBuffer(), 0, data, 0, data.Length);
            }

            var request = new OffsetFetchRequest {ApiVersion = 0};
            var responses = request.Decode(data); 
                // doesn't include the size in the decode -- the framework deals with it, I'd assume
            data.PrefixWithInt32Length().AssertProtocol(
                     reader => {
                         reader.AssertResponseHeader(correlationId);
                         reader.AssertOffsetFetchResponse(responses);
                     });
        }

        /// <summary>
        /// GroupCoordinatorRequest => GroupId
        ///  GroupId => string -- The consumer group id.
        ///
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
        /// </summary>
        [Test]
        public void GroupCoordinatorApiRequest([Values("group1", "group2")] string groupId)
        {
            var clientId = "GroupCoordinatorApiRequest";

            var request = new ConsumerMetadataRequest {
                ClientId = clientId,
                CorrelationId = clientId.GetHashCode(),
                ConsumerGroup = groupId,
                ApiVersion = 0
            };

            var data = request.Encode();

            data.Buffer.AssertProtocol(
                     reader =>
                     {
                         reader.AssertRequestHeader(request);
                         reader.AssertGroupCoordinatorRequest(request);
                     });
        }

        /// <summary>
        /// GroupCoordinatorResponse => ErrorCode CoordinatorId CoordinatorHost CoordinatorPort
        ///  ErrorCode => int16        -- The error code.
        ///  CoordinatorId => int32    -- The broker id.
        ///  CoordinatorHost => string -- The hostname of the broker.
        ///  CoordinatorPort => int32  -- The port on which the broker accepts requests.
        ///
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
        /// </summary>
        [Test]
        public void GroupCoordinatorApiResponse(
            [Values(
                 ErrorResponseCode.NoError,
                 ErrorResponseCode.ConsumerCoordinatorNotAvailableCode,
                 ErrorResponseCode.GroupAuthorizationFailed
             )] ErrorResponseCode errorCode,
            [Values(0, 1)] int coordinatorId
            )
        {
            var clientId = "GroupCoordinatorApiResponse";
            var correlationId = clientId.GetHashCode();

            byte[] data = null;
            using (var stream = new MemoryStream()) {
                var writer = new BigEndianBinaryWriter(stream);
                writer.WriteResponseHeader(correlationId);
                writer.Write((short)errorCode);
                writer.Write(coordinatorId);
                writer.Write("broker-" + coordinatorId, StringPrefixEncoding.Int16);
                writer.Write(9092 + coordinatorId);

                data = new byte[stream.Position];
                Buffer.BlockCopy(stream.GetBuffer(), 0, data, 0, data.Length);
            }

            var request = new ConsumerMetadataRequest {ApiVersion = 0};
            var responses = request.Decode(data).Single(); 
                // doesn't include the size in the decode -- the framework deals with it, I'd assume
            data.PrefixWithInt32Length().AssertProtocol(
                     reader => {
                         reader.AssertResponseHeader(correlationId);
                         reader.AssertGroupCoordinatorResponse(responses);
                     });
        }

        private List<Message> GenerateMessages(int count, byte version)
        {
            var randomizer = new Randomizer();
            var messages = new List<Message>();
            for (var m = 0; m < count; m++) {
                var message = new Message {
                    MagicNumber = version,
                    Timestamp = DateTime.UtcNow,
                    Key = m > 0 ? new byte[8] : null,
                    Value = new byte[8*(m + 1)]
                };
                if (message.Key != null) {
                    randomizer.NextBytes(message.Key);
                }
                randomizer.NextBytes(message.Value);
                messages.Add(message);
            }
            return messages;
        }
    }
}