using System;
using System.Collections.Generic;
using System.Linq;
using KafkaNet.Common;

namespace KafkaNet.Protocol
{
    public class FetchRequest : BaseRequest, IKafkaRequest<FetchResponse>
    {
        internal const int DefaultMinBlockingByteBufferSize = 4096;
        private const int DefaultMaxBlockingWaitTime = 5000;

        /// <summary>
        /// Indicates the type of kafka encoding this request is
        /// </summary>
        public ApiKeyRequestType ApiKey { get { return ApiKeyRequestType.Fetch; } }
        /// <summary>
        /// The maximum amount of time to block for the MinBytes to be available before returning.
        /// </summary>
        public int MaxWaitTime = DefaultMaxBlockingWaitTime;
        /// <summary>
        /// Defines how many bytes should be available before returning data. A value of 0 indicate a no blocking command.
        /// </summary>
        public int MinBytes = DefaultMinBlockingByteBufferSize;
        public List<Fetch> Fetches { get; set; }

        public byte[] Encode()
        {
            return EncodeFetchRequest(this);
        }

        public IEnumerable<FetchResponse> Decode(byte[] payload)
        {
            return DecodeFetchResponse(payload);
        }

        private byte[] EncodeFetchRequest(FetchRequest request)
        {
            var message = new WriteByteStream();
            if (request.Fetches == null) request.Fetches = new List<Fetch>();

            message.Pack(EncodeHeader(request));

            var topicGroups = request.Fetches.GroupBy(x => x.Topic).ToList();
            message.Pack(ReplicaId.ToBytes(), request.MaxWaitTime.ToBytes(), request.MinBytes.ToBytes(), topicGroups.Count.ToBytes());

            foreach (var topicGroup in topicGroups)
            {
                var partitions = topicGroup.GroupBy(x => x.PartitionId).ToList();
                message.Pack(topicGroup.Key.ToInt16SizedBytes(), partitions.Count.ToBytes());

                foreach (var partition in partitions)
                {
                    foreach (var fetch in partition)
                    {
                        message.Pack(partition.Key.ToBytes(), fetch.Offset.ToBytes(), fetch.MaxBytes.ToBytes());
                    }
                }
            }

            message.Prepend(message.Length().ToBytes());

            return message.Payload();
        }

        private IEnumerable<FetchResponse> DecodeFetchResponse(byte[] data)
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
                    var partitionId = stream.ReadInt();
                    var response = new FetchResponse
                    {
                        Topic = topic,
                        PartitionId = partitionId,
                        Error = stream.ReadInt16(),
                        HighWaterMark = stream.ReadLong()
                    };
                    //note: dont use initializer here as it breaks stream position.
                    response.Messages = Message.DecodeMessageSet(stream.ReadIntPrefixedBytes())
                        .Select(x => { x.Meta.PartitionId = partitionId; return x; })
                        .ToList();
                    yield return response;
                }
            }
        }
    }

    public class Fetch
    {
        public Fetch()
        {
            MaxBytes = FetchRequest.DefaultMinBlockingByteBufferSize * 8;
        }

        public string Topic { get; set; }

        public int PartitionId { get; set; }

        public long Offset { get; set; }

        public int MaxBytes { get; set; }
    }

    public class FetchResponse
    {
        public string Topic { get; set; }
        public int PartitionId { get; set; }
        public Int16 Error { get; set; }
        public long HighWaterMark { get; set; }
        public List<Message> Messages { get; set; }

        public FetchResponse()
        {
            Messages = new List<Message>();
        }
    }
}