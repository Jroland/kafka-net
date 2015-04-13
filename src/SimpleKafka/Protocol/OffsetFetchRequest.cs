using System;
using System.Collections.Generic;
using System.Linq;

using SimpleKafka.Common;

namespace SimpleKafka.Protocol
{
    /// <summary>
    /// Class that represents both the request and the response from a kafka server of requesting a stored offset value
    /// for a given consumer group.  Essentially this part of the api allows a user to save/load a given offset position
    /// under any abritrary name.
    /// This now supports version 1 of the protocol
    /// </summary>
    public class OffsetFetchRequest : BaseRequest<List<OffsetFetchResponse>>, IKafkaRequest
    {
        public OffsetFetchRequest(short version = 1) : base(ApiKeyRequestType.OffsetFetch, version)
        {

        }
        public string ConsumerGroup { get; set; }
        public List<OffsetFetch> Topics { get; set; }

        internal override KafkaEncoder Encode(KafkaEncoder encoder)
        {
            return EncodeOffsetFetchRequest(this, encoder);
        }

        private static KafkaEncoder EncodeOffsetFetchRequest(OffsetFetchRequest request, KafkaEncoder encoder)
        {
            request
                .EncodeHeader(encoder)
                .Write(request.ConsumerGroup);

            if (request.Topics == null)
            {
                // nothing to fetch
                encoder.Write(0);
            }
            else if (request.Topics.Count == 1)
            {
                // Short cut single instance request
                var fetch = request.Topics[0];
                encoder
                    .Write(1)
                    .Write(fetch.Topic)
                    .Write(1)
                    .Write(fetch.PartitionId);
            }
            else
            {
                // more complex
                var topicGroups = new Dictionary<string, List<int>>();
                foreach (var fetch in request.Topics)
                {
                    var partitions = topicGroups.GetOrCreate(fetch.Topic, () => new List<int>(request.Topics.Count));
                    partitions.Add(fetch.PartitionId);
                }

                encoder.Write(topicGroups.Count);
                foreach (var kvp in topicGroups)
                {
                    var topic = kvp.Key;
                    var partitions = kvp.Value;
                    encoder
                        .Write(topic)
                        .Write(partitions.Count);
                    foreach (var fetch in partitions)
                    {
                        encoder.Write(fetch);
                    }

                }
            }

            return encoder;
        }

        internal override List<OffsetFetchResponse> Decode(KafkaDecoder decoder)
        {
            return DecodeOffsetFetchResponse(decoder);
        }


        private static List<OffsetFetchResponse> DecodeOffsetFetchResponse(KafkaDecoder decoder)
        {
            var correlationId = decoder.ReadInt32();

            var responses = new List<OffsetFetchResponse>();
            var topicCount = decoder.ReadInt32();
            for (int i = 0; i < topicCount; i++)
            {
                var topic = decoder.ReadString();

                var partitionCount = decoder.ReadInt32();
                for (int j = 0; j < partitionCount; j++)
                {
                    var response = OffsetFetchResponse.Decode(decoder, topic);
                    responses.Add(response);
                }
            }
            return responses;
        }

    }

    public class OffsetFetch
    {
        /// <summary>
        /// The topic the offset came from.
        /// </summary>
        public string Topic { get; set; }
        /// <summary>
        /// The partition the offset came from.
        /// </summary>
        public int PartitionId { get; set; }
    }

    public class OffsetFetchResponse
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
        /// The offset position saved to the server.
        /// </summary>
        public readonly long Offset;
        /// <summary>
        /// Any arbitrary metadata stored during a CommitRequest.
        /// </summary>
        public readonly string MetaData;
        /// <summary>
        /// Error code of exception that occured during the request.  Zero if no error.
        /// </summary>
        public readonly ErrorResponseCode Error;

        private OffsetFetchResponse(string topic, int partitionId, long offset, string metaData, ErrorResponseCode error)
        {
            this.Topic = topic;
            this.PartitionId = partitionId;
            this.Offset = offset;
            this.MetaData = metaData;
            this.Error = error;
        }

        internal static OffsetFetchResponse Decode(KafkaDecoder decoder, string topic)
        {
            var partitionId = decoder.ReadInt32();
            var offset = decoder.ReadInt64();
            var metaData = decoder.ReadString();
            var error = decoder.ReadErrorResponseCode();
            var response = new OffsetFetchResponse(topic, partitionId, offset, metaData, error);
            return response;
        }

        public override string ToString()
        {
            return string.Format("[OffsetFetchResponse TopicName={0}, PartitionID={1}, Offset={2}, MetaData={3}, ErrorCode={4}]", Topic, PartitionId, Offset, MetaData, Error);
        }

    }
}
