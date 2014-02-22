using System;
using System.Collections.Generic;
using System.Linq;
using KafkaNet.Common;

namespace KafkaNet.Protocol
{
    public class FetchRequest : BaseRequest, IKafkaRequest<FetchResponse>
    {
        /// <summary>
        /// Indicates the type of kafka encoding this request is
        /// </summary>
        public ApiKeyRequestType ApiKey { get { return ApiKeyRequestType.Fetch; } }
        public int MaxWaitTime = 100;
        public int MinBytes = 4096;
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
                    var response = new FetchResponse
                    {
                        Topic = topic,
                        PartitionId = stream.ReadInt(),
                        Error = stream.ReadInt16(),
                        HighWaterMark = stream.ReadLong()
                    };
                    response.Messages = Message.DecodeMessageSet(stream.ReadIntPrefixedBytes()).ToList();
                    yield return response;
                }
            }
        }
    }

    public class Fetch
    {
        public Fetch()
        {
            MaxBytes = 4096;
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
    }
}