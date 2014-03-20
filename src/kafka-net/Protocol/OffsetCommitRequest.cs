using System;
using System.Collections.Generic;
using System.Linq;
using KafkaNet.Common;

namespace KafkaNet.Protocol
{
    public class OffsetCommitRequest : BaseRequest, IKafkaRequest<OffsetCommitResponse>
    {
        public ApiKeyRequestType ApiKey { get { return ApiKeyRequestType.OffsetCommit; } }
        public string ConsumerGroup { get; set; }
        public List<OffsetCommit> OffsetCommits { get; set; }

        public byte[] Encode()
        {
            return EncodeOffsetCommitRequest(this);
        }

        public IEnumerable<OffsetCommitResponse> Decode(byte[] payload)
        {
            return DecodeOffsetCommitResponse(payload);
        }

        private byte[] EncodeOffsetCommitRequest(OffsetCommitRequest request)
        {
            var message = new WriteByteStream();
            if (request.OffsetCommits == null) request.OffsetCommits = new List<OffsetCommit>();

            message.Pack(EncodeHeader(request));
            message.Pack(request.ConsumerGroup.ToInt16SizedBytes());

            var topicGroups = request.OffsetCommits.GroupBy(x => x.Topic).ToList();
            message.Pack(topicGroups.Count.ToBytes());

            foreach (var topicGroup in topicGroups)
            {
                var partitions = topicGroup.GroupBy(x => x.PartitionId).ToList();
                message.Pack(topicGroup.Key.ToInt16SizedBytes(), partitions.Count.ToBytes());

                foreach (var partition in partitions)
                {
                    foreach (var commit in partition)
                    {
                        message.Pack(partition.Key.ToBytes(), commit.Offset.ToBytes(), commit.Metadata.ToInt16SizedBytes());
                    }
                }
            }

            message.Prepend(message.Length().ToBytes());

            return message.Payload();
        }

        private IEnumerable<OffsetCommitResponse> DecodeOffsetCommitResponse(byte[] data)
        {
            var stream = new ReadByteStream(data);

            var correlationId = stream.ReadInt();

            var topicCount = stream.ReadInt();
            for (int i = 0; i < topicCount; i++)
            {
                var topic = stream.ReadInt16String();

                var partitionCount = stream.ReadInt();
                for (int j = 0; j < partitionCount; j++)
                {
                    var response = new OffsetCommitResponse()
                    {
                        Topic = topic,
                        PartitionId = stream.ReadInt(),
                        Error = stream.ReadInt16()
                    };

                    yield return response;
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
        /// Descriptive metadata about this commit.
        /// </summary>
        public string Metadata { get; set; }
    }

    public class OffsetCommitResponse
    {
        public string Topic { get; set; }
        public int PartitionId { get; set; }
        public Int16 Error { get; set; }
    }
}