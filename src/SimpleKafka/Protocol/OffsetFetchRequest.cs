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
    public class OffsetFetchRequest : BaseRequest, IKafkaRequest<List<OffsetFetchResponse>>
    {
        public OffsetFetchRequest(short version = 1) : base(version)
        {

        }
        public ApiKeyRequestType ApiKey { get { return ApiKeyRequestType.OffsetFetch; } }
        public string ConsumerGroup { get; set; }
        public List<OffsetFetch> Topics { get; set; }

        public void Encode(ref KafkaEncoder encoder)
        {
            EncodeOffsetFetchRequest(this, ref encoder);
        }

        private static void EncodeOffsetFetchRequest(OffsetFetchRequest request, ref KafkaEncoder encoder)
        {
            if (request.Topics == null) request.Topics = new List<OffsetFetch>();
            EncodeHeader(request, ref encoder);

            encoder.Write(request.ConsumerGroup, StringPrefixEncoding.Int16);

            var topicGroups = request.Topics.GroupBy(x => x.Topic).ToList();
            encoder.Write(topicGroups.Count);

            foreach (var topicGroup in topicGroups)
            {
                var partitions = topicGroup.GroupBy(x => x.PartitionId).ToList();
                encoder.Write(topicGroup.Key, StringPrefixEncoding.Int16);
                encoder.Write(partitions.Count);

                foreach (var partition in partitions)
                {
                    foreach (var offset in partition)
                    {
                        encoder.Write(offset.PartitionId);
                    }
                }
            }

        }

        public List<OffsetFetchResponse> Decode(ref KafkaDecoder decoder)
        {
            return DecodeOffsetFetchResponse(ref decoder);
        }


        private static List<OffsetFetchResponse> DecodeOffsetFetchResponse(ref KafkaDecoder decoder)
        {
            var correlationId = decoder.ReadInt32();

            var responses = new List<OffsetFetchResponse>();
            var topicCount = decoder.ReadInt32();
            for (int i = 0; i < topicCount; i++)
            {
                var topic = decoder.ReadInt16String();

                var partitionCount = decoder.ReadInt32();
                for (int j = 0; j < partitionCount; j++)
                {
                    var response = new OffsetFetchResponse()
                    {
                        Topic = topic,
                        PartitionId = decoder.ReadInt32(),
                        Offset = decoder.ReadInt64(),
                        MetaData = decoder.ReadInt16String(),
                        Error = decoder.ReadInt16()
                    };
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
        public string Topic;
        /// <summary>
        /// The id of the partition this response is for.
        /// </summary>
        public Int32 PartitionId;
        /// <summary>
        /// The offset position saved to the server.
        /// </summary>
        public Int64 Offset;
        /// <summary>
        /// Any arbitrary metadata stored during a CommitRequest.
        /// </summary>
        public string MetaData;
        /// <summary>
        /// Error code of exception that occured during the request.  Zero if no error.
        /// </summary>
        public Int16 Error;

        public override string ToString()
        {
            return string.Format("[OffsetFetchResponse TopicName={0}, PartitionID={1}, Offset={2}, MetaData={3}, ErrorCode={4}]", Topic, PartitionId, Offset, MetaData, Error);
        }

    }
}
