using System;
using System.Collections.Generic;
using System.Linq;
using SimpleKafka.Common;

namespace SimpleKafka.Protocol
{
    /// <summary>
    /// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetFetchRequest
    /// Class that represents the api call to commit a specific set of offsets for a given topic.  The offset is saved under the 
    /// arbitrary ConsumerGroup name provided by the call.
    /// This now supports version 0 and 1 of the protocol
    /// </summary>
    public class OffsetCommitRequest : BaseRequest<List<OffsetCommitResponse>>, IKafkaRequest
    {
        public OffsetCommitRequest(Int16 version = 1)
            : base(ApiKeyRequestType.OffsetCommit, version)
        {
        }
        public string ConsumerGroup { get; set; }
        public int ConsumerGroupGenerationId { get; set; }
        public string ConsumerId { get; set; }
        public List<OffsetCommit> OffsetCommits { get; set; }

        internal override KafkaEncoder Encode(KafkaEncoder encoder)
        {
            return EncodeOffsetCommitRequest(this, encoder);
        }

        internal override List<OffsetCommitResponse> Decode(KafkaDecoder decoder)
        {
            return DecodeOffsetCommitResponse(decoder);
        }

        private static KafkaEncoder EncodeOffsetCommitRequest(OffsetCommitRequest request, KafkaEncoder encoder)
        {
            request
                .EncodeHeader(encoder)
                .Write(request.ConsumerGroup);

            if (request.ApiVersion == 1)
            {
                encoder
                    .Write(request.ConsumerGroupGenerationId)
                    .Write(request.ConsumerId);
            }

            if (request.OffsetCommits == null)
            {
                // Nothing to commit
                encoder.Write(0);
            }
            else if (request.OffsetCommits.Count == 1)
            {
                var commit = request.OffsetCommits[0];
                // Shortcut the single version
                encoder
                    .Write(1)
                    .Write(commit.Topic)
                    .Write(1);

                EncodeCommit(encoder, request.ApiVersion, commit);
            }
            else
            {
                // Complete complex request
                var topicGroups = new Dictionary<string, List<OffsetCommit>>();
                foreach (var commit in request.OffsetCommits)
                {
                    var topicGroup = topicGroups.GetOrCreate(commit.Topic, () => new List<OffsetCommit>(request.OffsetCommits.Count));
                    topicGroup.Add(commit);
                }

                encoder.Write(topicGroups.Count);
                foreach (var topicGroupKvp in topicGroups)
                {
                    var topic = topicGroupKvp.Key;
                    var commits = topicGroupKvp.Value;
                    encoder
                        .Write(topic)
                        .Write(commits.Count);

                    foreach (var commit in commits)
                    {
                        EncodeCommit(encoder, request.ApiVersion, commit);
                    }
                }
            }
            return encoder;
        }

        private static void EncodeCommit(KafkaEncoder encoder, int apiVersion, OffsetCommit commit)
        {
            encoder
                .Write(commit.PartitionId)
                .Write(commit.Offset);

            if (apiVersion == 1)
            {
                encoder.Write(commit.TimeStamp);
            }
            encoder.Write(commit.Metadata);
        }

        private static List<OffsetCommitResponse> DecodeOffsetCommitResponse(KafkaDecoder decoder)
        {
            var correlationId = decoder.ReadInt32();

            var responses = new List<OffsetCommitResponse>();
            var topicCount = decoder.ReadInt32();
            for (int i = 0; i < topicCount; i++)
            {
                var topic = decoder.ReadString();

                var partitionCount = decoder.ReadInt32();
                for (int j = 0; j < partitionCount; j++)
                {
                    var response = OffsetCommitResponse.Decode(decoder, topic);
                    responses.Add(response);
                }
            }
            return responses;
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
        public readonly string Topic;
        /// <summary>
        /// The id of the partition this response is for.
        /// </summary>
        public readonly int PartitionId;
        /// <summary>
        /// Error code of exception that occured during the request.  Zero if no error.
        /// </summary>
        public readonly ErrorResponseCode Error;

        private OffsetCommitResponse(string topic, int partitionId, ErrorResponseCode error)
        {
            this.Topic = topic;
            this.PartitionId = partitionId;
            this.Error = error;
        }

        internal static OffsetCommitResponse Decode(KafkaDecoder decoder, string topic)
        {
            var partitionId = decoder.ReadInt32();
            var error = decoder.ReadErrorResponseCode();
            var response = new OffsetCommitResponse(topic, partitionId, error);
            return response;
        }
    }
}