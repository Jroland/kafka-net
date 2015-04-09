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
    public class OffsetCommitRequest : BaseRequest, IKafkaRequest<List<OffsetCommitResponse>>
    {
        public OffsetCommitRequest(Int16 version = 1) : base(version)
        {
        }
        public ApiKeyRequestType ApiKey { get { return ApiKeyRequestType.OffsetCommit; } }
        public string ConsumerGroup { get; set; }
        public int ConsumerGroupGenerationId { get; set; }
        public string ConsumerId { get; set; }
        public List<OffsetCommit> OffsetCommits { get; set; }

        public void Encode(ref BigEndianEncoder encoder)
        {
            EncodeOffsetCommitRequest(this, ref encoder);
        }

        public List<OffsetCommitResponse> Decode(ref BigEndianDecoder decoder)
        {
            return DecodeOffsetCommitResponse(ref decoder);
        }

        private static void EncodeOffsetCommitRequest(OffsetCommitRequest request, ref BigEndianEncoder encoder)
        {
            if (request.OffsetCommits == null) request.OffsetCommits = new List<OffsetCommit>();
            EncodeHeader(request, ref encoder);
            encoder.Write(request.ConsumerGroup, StringPrefixEncoding.Int16);
            if (request.ApiVersion == 1)
            {
                encoder.Write(request.ConsumerGroupGenerationId);
                encoder.Write(request.ConsumerId, StringPrefixEncoding.Int16);
            }

            var topicGroups = request.OffsetCommits.GroupBy(x => x.Topic).ToList();
            encoder.Write(topicGroups.Count);

            foreach (var topicGroup in topicGroups)
            {
                var partitions = topicGroup.GroupBy(x => x.PartitionId).ToList();
                encoder.Write(topicGroup.Key, StringPrefixEncoding.Int16);
                encoder.Write(partitions.Count);

                foreach (var partition in partitions)
                {
                    foreach (var commit in partition)
                    {
                        encoder.Write(partition.Key);
                        encoder.Write(commit.Offset);

                        if (request.ApiVersion == 1)
                        {
                            encoder.Write(commit.TimeStamp);
                        }

                        encoder.Write(commit.Metadata, StringPrefixEncoding.Int16);
                    }
                }
            }
        }

        private static List<OffsetCommitResponse> DecodeOffsetCommitResponse(ref BigEndianDecoder decoder)
        {
            var correlationId = decoder.ReadInt32();

            var responses = new List<OffsetCommitResponse>();
            var topicCount = decoder.ReadInt32();
            for (int i = 0; i < topicCount; i++)
            {
                var topic = decoder.ReadInt16String();

                var partitionCount = decoder.ReadInt32();
                for (int j = 0; j < partitionCount; j++)
                {
                    var response = new OffsetCommitResponse()
                    {
                        Topic = topic,
                        PartitionId = decoder.ReadInt32(),
                        Error = decoder.ReadInt16()
                    };
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