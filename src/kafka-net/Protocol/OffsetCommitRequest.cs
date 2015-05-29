using System;
using System.Collections.Generic;
using System.Linq;
using KafkaNet.Common;

namespace KafkaNet.Protocol
{
    /// <summary>
    /// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetFetchRequest
    /// https://cwiki.apache.org/confluence/display/KAFKA/Kafka+0.9+Consumer+Rewrite+Design
    /// Class that represents the api call to commit a specific set of offsets for a given topic.  The offset is saved under the 
    /// arbitrary ConsumerGroup name provided by the call.
    /// This now supports version 0 and 1 of the protocol
    /// </summary>
    public class OffsetCommitRequest : BaseRequest, IKafkaRequest<OffsetCommitResponse>
    {
        public OffsetCommitRequest(Int16 version = SupportedApiVersion.ApiV1)
            : base(version)
        {
        }

        public ApiKeyRequestType ApiKey { get { return ApiKeyRequestType.OffsetCommit; } }
        /// <summary>
        /// Name for the entire consumer group cluster.
        /// </summary>
        public string ConsumerGroup { get; set; }
        /// <summary>
        /// The Generation number received by JoinGroupResponse which indicates the version of the assigned consumer partitions. 
        /// </summary>
        public int ConsumerGroupGenerationId { get; set; }
        /// <summary>
        /// The unique id of the individual consumer within the group received by a JoinGroupResponse.
        /// </summary>
        public string ConsumerId { get; set; }
        /// <summary>
        /// The offset information to commit to the brokers.
        /// </summary>
        public List<OffsetCommit> OffsetCommits { get; set; }

        public KafkaDataPayload Encode()
        {
            return EncodeOffsetCommitRequest(this);
        }

        public IEnumerable<OffsetCommitResponse> Decode(byte[] payload)
        {
            return DecodeOffsetCommitResponse(payload);
        }

        private KafkaDataPayload EncodeOffsetCommitRequest(OffsetCommitRequest request)
        {
            if (request.OffsetCommits == null) request.OffsetCommits = new List<OffsetCommit>();

            using (var message = EncodeHeader(request).Pack(request.ConsumerGroup, StringPrefixEncoding.Int16))
            {
                if (ApiVersion == SupportedApiVersion.ApiV1)
                {
                    message
                        .Pack(ConsumerGroupGenerationId)
                        .Pack(ConsumerId, StringPrefixEncoding.Int16);
                }
                var topicGroups = request.OffsetCommits.GroupBy(x => x.Topic).ToList();
                message.Pack(topicGroups.Count);

                foreach (var topicGroup in topicGroups)
                {
                    var partitions = topicGroup.GroupBy(x => x.PartitionId).ToList();
                    message.Pack(topicGroup.Key, StringPrefixEncoding.Int16)
                        .Pack(partitions.Count);

                    foreach (var partition in partitions)
                    {
                        foreach (var commit in partition)
                        {
                            message
                                .Pack(partition.Key)
                                .Pack(commit.Offset);

                            if (ApiVersion == SupportedApiVersion.ApiV1)
                            {
                                message.Pack(commit.TimeStamp);
                            }

                            message
                                .Pack(commit.Metadata, StringPrefixEncoding.Int16);
                        }
                    }
                }

                return new KafkaDataPayload
                {
                    Buffer = message.Payload(),
                    CorrelationId = request.CorrelationId,
                    ApiKey = ApiKey
                };
            }
        }

        private IEnumerable<OffsetCommitResponse> DecodeOffsetCommitResponse(byte[] data)
        {
            using (var stream = new BigEndianBinaryReader(data))
            {
                var correlationId = stream.ReadInt32();

                var topicCount = stream.ReadInt32();
                for (int i = 0; i < topicCount; i++)
                {
                    var topic = stream.ReadInt16String();

                    var partitionCount = stream.ReadInt32();
                    for (int j = 0; j < partitionCount; j++)
                    {
                        var response = new OffsetCommitResponse()
                        {
                            Topic = topic,
                            PartitionId = stream.ReadInt32(),
                            Error = stream.ReadInt16()
                        };

                        yield return response;
                    }
                }
            }
        }
    }

    public class OffsetCommit
    {
        /// <summary>
        /// The topic the offset came from.
        /// </summary>
        public string Topic { get; set; }
        /// <summary>
        /// The partition the offset came from.
        /// </summary>
        public int PartitionId { get; set; }
        /// <summary>
        /// The offset number to commit as completed.
        /// </summary>
        public long Offset { get; set; }
        /// <summary>
        /// If the time stamp field is set to -1, then the broker sets the time stamp to the receive time before committing the offset.
        /// </summary>
        public long TimeStamp { get; set; }
        /// <summary>
        /// Descriptive metadata about this commit.
        /// </summary>
        public string Metadata { get; set; }

        public OffsetCommit()
        {
            TimeStamp = -1;
        }
    
    }

    public class OffsetCommitResponse
    {
        /// <summary>
        /// The name of the topic this response entry is for.
        /// </summary>
        public string Topic;
        /// <summary>
        /// The id of the partition this response is for.
        /// </summary>
        public Int32 PartitionId;
        /// <summary>
        /// Error code of exception that occured during the request.  Zero if no error.
        /// </summary>
        public Int16 Error;
    }
}