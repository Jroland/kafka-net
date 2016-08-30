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
        /// <summary>
        /// GroupCoordinatorResponse => ErrorCode CoordinatorId CoordinatorHost CoordinatorPort
        ///  ErrorCode => int16        -- The error code.
        ///  CoordinatorId => int32    -- The broker id.
        ///  CoordinatorHost => string -- The hostname of the broker.
        ///  CoordinatorPort => int32  -- The port on which the broker accepts requests.
        ///
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
        /// </summary>
        public static void AssertGroupCoordinatorResponse(this BigEndianBinaryReader reader, ConsumerMetadataResponse response)
        {
            Assert.That(reader.ReadInt16(), Is.EqualTo(response.Error), "ErrorCode");
            Assert.That(reader.ReadInt32(), Is.EqualTo(response.CoordinatorId), "CoordinatorId");
            Assert.That(reader.ReadInt16String(), Is.EqualTo(response.CoordinatorHost), "CoordinatorHost");
            Assert.That(reader.ReadInt32(), Is.EqualTo(response.CoordinatorPort), "CoordinatorPort");
        }

        /// <summary>
        /// GroupCoordinatorRequest => GroupId
        ///  GroupId => string -- The consumer group id.
        ///
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
        /// </summary>
        public static void AssertGroupCoordinatorRequest(this BigEndianBinaryReader reader, ConsumerMetadataRequest request)
        {
            Assert.That(reader.ReadInt16String(), Is.EqualTo(request.ConsumerGroup), "ConsumerGroup");
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
        public static void AssertOffsetFetchResponse(this BigEndianBinaryReader reader, IEnumerable<OffsetFetchResponse> response)
        {
            var responses = response.GroupBy(r => r.Topic).ToList();
            Assert.That(reader.ReadInt32(), Is.EqualTo(responses.Count), "[TopicName]");
            foreach (var payload in responses) {
                Assert.That(reader.ReadInt16String(), Is.EqualTo(payload.Key), "TopicName");
                var partitions = payload.ToList();
                Assert.That(reader.ReadInt32(), Is.EqualTo(partitions.Count), "[Partition]");
                foreach (var partition in partitions) {
                    Assert.That(reader.ReadInt32(), Is.EqualTo(partition.PartitionId), "Partition");
                    Assert.That(reader.ReadInt64(), Is.EqualTo(partition.Offset), "Offset");
                    Assert.That(reader.ReadInt16String(), Is.EqualTo(partition.MetaData), "Metadata");
                    Assert.That(reader.ReadInt16(), Is.EqualTo((short)partition.Error), "ErrorCode");
                }
            }
        }

        /// <summary>
        /// OffsetFetchRequest => ConsumerGroup [TopicName [Partition]]
        ///  ConsumerGroup => string -- The consumer group id.
        ///  TopicName => string     -- The topic to commit.
        ///  Partition => int32      -- The partition id.
        ///
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
        /// </summary>
        public static void AssertOffsetFetchRequest(this BigEndianBinaryReader reader, OffsetFetchRequest request)
        {
            Assert.That(reader.ReadInt16String(), Is.EqualTo(request.ConsumerGroup), "ConsumerGroup");

            Assert.That(reader.ReadInt32(), Is.EqualTo(request.Topics.Count), "[TopicName]");
            foreach (var payload in request.Topics) {
                Assert.That(reader.ReadInt16String(), Is.EqualTo(payload.Topic), "TopicName");
                Assert.That(reader.ReadInt32(), Is.EqualTo(1), "[Partition]"); // this is a mismatch between the protocol and the object model
                Assert.That(reader.ReadInt32(), Is.EqualTo(payload.PartitionId), "Partition");
            }
        }

        /// <summary>
        /// OffsetCommitResponse => [TopicName [Partition ErrorCode]]]
        ///  TopicName => string -- The name of the topic.
        ///  Partition => int32  -- The id of the partition.
        ///  ErrorCode => int16  -- The error code for the partition, if any.
        ///
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
        /// </summary>
        public static void AssertOffsetCommitResponse(this BigEndianBinaryReader reader, IEnumerable<OffsetCommitResponse> response)
        {
            var responses = response.GroupBy(r => r.Topic).ToList();
            Assert.That(reader.ReadInt32(), Is.EqualTo(responses.Count), "[TopicName]");
            foreach (var payload in responses) {
                Assert.That(reader.ReadInt16String(), Is.EqualTo(payload.Key), "TopicName");
                var partitions = payload.ToList();
                Assert.That(reader.ReadInt32(), Is.EqualTo(partitions.Count), "[Partition]");
                foreach (var partition in partitions) {
                    Assert.That(reader.ReadInt32(), Is.EqualTo(partition.PartitionId), "Partition");
                    Assert.That(reader.ReadInt16(), Is.EqualTo((short)partition.Error), "ErrorCode");
                }
            }
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
        public static void AssertOffsetCommitRequest(this BigEndianBinaryReader reader, OffsetCommitRequest request)
        {
            Assert.That(reader.ReadInt16String(), Is.EqualTo(request.ConsumerGroup), "ConsumerGroup");

            if (request.ApiVersion >= 1) {
                Assert.That(reader.ReadInt32(), Is.EqualTo(request.GenerationId), "ConsumerGroupGenerationId");
                Assert.That(reader.ReadInt16String(), Is.EqualTo(request.MemberId), "MemberId");                
            }
            if (request.ApiVersion >= 2) {
                var expectedRetention = request.OffsetRetention.HasValue
                                            ? (long) request.OffsetRetention.Value.TotalMilliseconds
                                            : -1L;
                Assert.That(reader.ReadInt64(), Is.EqualTo(expectedRetention), "RetentionTime");
            }

            Assert.That(reader.ReadInt32(), Is.EqualTo(request.OffsetCommits.Count), "[TopicName]");
            foreach (var payload in request.OffsetCommits) {
                Assert.That(reader.ReadInt16String(), Is.EqualTo(payload.Topic), "TopicName");
                Assert.That(reader.ReadInt32(), Is.EqualTo(1), "[Partition]"); // this is a mismatch between the protocol and the object model
                Assert.That(reader.ReadInt32(), Is.EqualTo(payload.PartitionId), "Partition");
                Assert.That(reader.ReadInt64(), Is.EqualTo(payload.Offset), "Offset");

                if (request.ApiVersion == 1) {
                    Assert.That(reader.ReadInt64(), Is.EqualTo(payload.TimeStamp), "TimeStamp");
                }
                Assert.That(reader.ReadInt16String(), Is.EqualTo(payload.Metadata), "Metadata");
            }
        }

        /// <summary>
        /// MetadataResponse => [Broker][TopicMetadata]
        ///  Broker => NodeId Host Port  (any number of brokers may be returned)
        ///                              -- The node id, hostname, and port information for a kafka broker
        ///   NodeId => int32
        ///   Host => string
        ///   Port => int32
        ///  TopicMetadata => TopicErrorCode TopicName [PartitionMetadata]
        ///   TopicErrorCode => int16
        ///  PartitionMetadata => PartitionErrorCode PartitionId Leader Replicas Isr
        ///   PartitionErrorCode => int16
        ///   PartitionId => int32
        ///   Leader => int32             -- The node id for the kafka broker currently acting as leader for this partition.
        ///                                  If no leader exists because we are in the middle of a leader election this id will be -1.
        ///   Replicas => [int32]         -- The set of alive nodes that currently acts as slaves for the leader for this partition.
        ///   Isr => [int32]              -- The set subset of the replicas that are "caught up" to the leader
        ///
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-MetadataAPI
        /// </summary>
        public static void AssertMetadataResponse(this BigEndianBinaryReader reader, MetadataResponse response)
        {
            Assert.That(reader.ReadInt32(), Is.EqualTo(response.Brokers.Count), "[Broker]");
            foreach (var payload in response.Brokers) {
                Assert.That(reader.ReadInt32(), Is.EqualTo(payload.BrokerId), "NodeId");
                Assert.That(reader.ReadInt16String(), Is.EqualTo(payload.Host), "Host");
                Assert.That(reader.ReadInt32(), Is.EqualTo(payload.Port), "Port");
            }
            Assert.That(reader.ReadInt32(), Is.EqualTo(response.Topics.Count), "[TopicMetadata]");
            foreach (var payload in response.Topics) {
                Assert.That(reader.ReadInt16(), Is.EqualTo((short)payload.ErrorCode), "TopicErrorCode");
                Assert.That(reader.ReadInt16String(), Is.EqualTo(payload.Name), "TopicName");
                Assert.That(reader.ReadInt32(), Is.EqualTo(payload.Partitions.Count), "[PartitionMetadata]");
                foreach (var partition in payload.Partitions) {
                    Assert.That(reader.ReadInt16(), Is.EqualTo((short) partition.ErrorCode), "PartitionErrorCode");
                    Assert.That(reader.ReadInt32(), Is.EqualTo(partition.PartitionId), "PartitionId");
                    Assert.That(reader.ReadInt32(), Is.EqualTo(partition.LeaderId), "Leader");
                    Assert.That(reader.ReadInt32(), Is.EqualTo(partition.Replicas.Count), "[Replicas]");
                    foreach (var replica in partition.Replicas) {
                        Assert.That(reader.ReadInt32(), Is.EqualTo(replica), "Replicas");
                    }
                    Assert.That(reader.ReadInt32(), Is.EqualTo(partition.Isrs.Count), "[Isr]");
                    foreach (var isr in partition.Isrs) {
                        Assert.That(reader.ReadInt32(), Is.EqualTo(isr), "Isr");
                    }
                }
            }
        }

        /// <summary>
        /// TopicMetadataRequest => [TopicName]
        ///  TopicName => string  -- The topics to produce metadata for. If empty the request will yield metadata for all topics.
        ///
        /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-MetadataAPI
        /// </summary>
        public static void AssertMetadataRequest(this BigEndianBinaryReader reader, MetadataRequest request)
        {
            Assert.That(reader.ReadInt32(), Is.EqualTo(request.Topics.Count), "[TopicName]");
            foreach (var payload in request.Topics) {
                Assert.That(reader.ReadInt16String(), Is.EqualTo(payload), "TopicName");
            }
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
        public static void AssertOffsetResponse(this BigEndianBinaryReader reader, IEnumerable<OffsetResponse> response)
        {
            var responses = response.ToList();
            Assert.That(reader.ReadInt32(), Is.EqualTo(responses.Count), "[TopicName]");
            foreach (var payload in responses) {
                Assert.That(reader.ReadInt16String(), Is.EqualTo(payload.Topic), "TopicName");
                Assert.That(reader.ReadInt32(), Is.EqualTo(1), "[Partition]"); // this is a mismatch between the protocol and the object model
                Assert.That(reader.ReadInt32(), Is.EqualTo(payload.PartitionId), "Partition");
                Assert.That(reader.ReadInt16(), Is.EqualTo(payload.Error), "ErrorCode");

                Assert.That(reader.ReadInt32(), Is.EqualTo(payload.Offsets.Count), "[Offset]");
                foreach (var offset in payload.Offsets) {
                    Assert.That(reader.ReadInt64(), Is.EqualTo(offset), "Offset");
                }
            }
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
        public static void AssertOffsetRequest(this BigEndianBinaryReader reader, OffsetRequest request)
        {
            Assert.That(reader.ReadInt32(), Is.EqualTo(-1), "ReplicaId");

            Assert.That(reader.ReadInt32(), Is.EqualTo(request.Offsets.Count), "[TopicName]");
            foreach (var payload in request.Offsets) {
                Assert.That(reader.ReadInt16String(), Is.EqualTo(payload.Topic), "TopicName");
                Assert.That(reader.ReadInt32(), Is.EqualTo(1), "[Partition]"); // this is a mismatch between the protocol and the object model
                Assert.That(reader.ReadInt32(), Is.EqualTo(payload.PartitionId), "Partition");

                Assert.That(reader.ReadInt64(), Is.EqualTo(payload.Time), "Time");
                Assert.That(reader.ReadInt32(), Is.EqualTo(payload.MaxOffsets), "MaxNumberOfOffsets");
            }
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
        public static void AssertFetchResponse(this BigEndianBinaryReader reader, int version, int throttleTime, IEnumerable<FetchResponse> response)
        {
            var responses = response.ToList();
            if (version >= 1) {
                Assert.That(reader.ReadInt32(), Is.EqualTo(throttleTime), "ThrottleTime");
            }
            Assert.That(reader.ReadInt32(), Is.EqualTo(responses.Count), "[TopicName]");
            foreach (var payload in responses) {
                Assert.That(reader.ReadInt16String(), Is.EqualTo(payload.Topic), "TopicName");
                Assert.That(reader.ReadInt32(), Is.EqualTo(1), "[Partition]"); // this is a mismatch between the protocol and the object model
                Assert.That(reader.ReadInt32(), Is.EqualTo(payload.PartitionId), "Partition");
                Assert.That(reader.ReadInt16(), Is.EqualTo(payload.Error), "Error");
                Assert.That(reader.ReadInt64(), Is.EqualTo(payload.HighWaterMark), "HighwaterMarkOffset");

                var finalPosition = reader.ReadInt32() + reader.Position;
                reader.AssertMessageSet(version, payload.Messages);
                Assert.That(reader.Position, Is.EqualTo(finalPosition),
                            string.Format("MessageSetSize was {0} but ended in a different spot.", finalPosition - 4));
            }
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
        public static void AssertFetchRequest(this BigEndianBinaryReader reader, FetchRequest request)
        {
            Assert.That(reader.ReadInt32(), Is.EqualTo(-1), "ReplicaId");
            Assert.That(reader.ReadInt32(), Is.EqualTo(request.MaxWaitTime), "MaxWaitTime");
            Assert.That(reader.ReadInt32(), Is.EqualTo(request.MinBytes), "MinBytes");

            Assert.That(reader.ReadInt32(), Is.EqualTo(request.Fetches.Count), "[TopicName]");
            foreach (var payload in request.Fetches) {
                Assert.That(reader.ReadInt16String(), Is.EqualTo(payload.Topic), "TopicName");
                Assert.That(reader.ReadInt32(), Is.EqualTo(1), "[Partition]"); // this is a mismatch between the protocol and the object model
                Assert.That(reader.ReadInt32(), Is.EqualTo(payload.PartitionId), "Partition");

                Assert.That(reader.ReadInt64(), Is.EqualTo(payload.Offset), "FetchOffset");
                Assert.That(reader.ReadInt32(), Is.EqualTo(payload.MaxBytes), "MaxBytes");
            }
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
        public static void AssertProduceResponse(this BigEndianBinaryReader reader, int version, int throttleTime, IEnumerable<ProduceResponse> response)
        {
            var responses = response.ToList();
            Assert.That(reader.ReadInt32(), Is.EqualTo(responses.Count), "[TopicName]");
            foreach (var payload in responses) {
                Assert.That(reader.ReadInt16String(), Is.EqualTo(payload.Topic), "TopicName");
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
                        Assert.That(payload.Timestamp.Value, Is.EqualTo(timestamp.FromUnixEpochMilliseconds()), "Timestamp");
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
                Assert.That(reader.ReadInt16String(), Is.EqualTo(payload.Topic), "TopicName");
                Assert.That(reader.ReadInt32(), Is.EqualTo(1), "[Partition]"); // this is a mismatch between the protocol and the object model
                Assert.That(reader.ReadInt32(), Is.EqualTo(payload.Partition), "Partition");

                var finalPosition = reader.ReadInt32() + reader.Position;
                reader.AssertMessageSet(request.ApiVersion, payload.Messages);
                Assert.That(reader.Position, Is.EqualTo(finalPosition),
                            string.Format("MessageSetSize was {0} but ended in a different spot.", finalPosition - 4));
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
                Assert.That(reader.Position, Is.EqualTo(finalPosition),
                            string.Format("MessageSize was {0} but ended in a different spot.", finalPosition - 4));
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
                Assert.That(reader.ReadInt64(), Is.EqualTo(message.Timestamp.ToUnixEpochMilliseconds()), "Timestamp");
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
            Assert.That(reader.ReadInt16String(), Is.EqualTo(clientId), "client_id");
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
                Assert.That(finalPosition, Is.EqualTo(reader.Position),
                            string.Format("Size was {0} but ended in a different spot.", finalPosition - 4));
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