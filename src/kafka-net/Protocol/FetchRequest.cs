using System;
using System.Collections.Generic;
using System.Linq;
using KafkaNet.Common;

namespace KafkaNet.Protocol
{
    public class FetchRequest : BaseRequest, IKafkaRequest<FetchResponse>
    {
        internal const int DefaultMinBlockingByteBufferSize = 4096;
        internal const int DefaultBufferSize = DefaultMinBlockingByteBufferSize * 8;
        internal const int DefaultMaxBlockingWaitTime = 5000;

        /// <summary>
        /// Indicates the type of kafka encoding this request is
        /// </summary>
        public ApiKeyRequestType ApiKey { get { return ApiKeyRequestType.Fetch; } }
        /// <summary>
        /// The max wait time is the maximum amount of time in milliseconds to block waiting if insufficient data is available at the time the request is issued.
        /// </summary>
        public int MaxWaitTime = DefaultMaxBlockingWaitTime;
        /// <summary>
        /// This is the minimum number of bytes of messages that must be available to give a response. 
        /// If the client sets this to 0 the server will always respond immediately, however if there is no new data since their last request they will just get back empty message sets. 
        /// If this is set to 1, the server will respond as soon as at least one partition has at least 1 byte of data or the specified timeout occurs. 
        /// By setting higher values in combination with the timeout the consumer can tune for throughput and trade a little additional latency for reading only large chunks of data 
        /// (e.g. setting MaxWaitTime to 100 ms and setting MinBytes to 64k would allow the server to wait up to 100ms to try to accumulate 64k of data before responding).
        /// </summary>
        public int MinBytes = DefaultMinBlockingByteBufferSize;

        public List<Fetch> Fetches { get; set; }

        public KafkaDataPayload Encode()
        {
            return EncodeFetchRequest(this);
        }

        public IEnumerable<FetchResponse> Decode(byte[] payload)
        {
            return DecodeFetchResponse(payload);
        }

        private KafkaDataPayload EncodeFetchRequest(FetchRequest request)
        {          
            if (request.Fetches == null) request.Fetches = new List<Fetch>();

            using (var message = EncodeHeader(request))
            {
                var topicGroups = request.Fetches.GroupBy(x => x.Topic).ToList();
                message.Pack(ReplicaId)
                    .Pack(request.MaxWaitTime)
                    .Pack(request.MinBytes)
                    .Pack(topicGroups.Count);

                foreach (var topicGroup in topicGroups)
                {
                    var partitions = topicGroup.GroupBy(x => x.PartitionId).ToList();
                    message.Pack(topicGroup.Key, StringPrefixEncoding.Int16)
                        .Pack(partitions.Count);

                    foreach (var partition in partitions)
                    {
                        foreach (var fetch in partition)
                        {
                            message.Pack(partition.Key)
                                .Pack(fetch.Offset)
                                .Pack(fetch.MaxBytes);
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

        private IEnumerable<FetchResponse> DecodeFetchResponse(byte[] data)
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
                        var partitionId = stream.ReadInt32();
                        var response = new FetchResponse
                        {
                            Topic = topic,
                            PartitionId = partitionId,
                            Error = stream.ReadInt16(),
                            HighWaterMark = stream.ReadInt64()
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
    }

    public class Fetch
    {
        public Fetch()
        {
            MaxBytes = FetchRequest.DefaultMinBlockingByteBufferSize * 8;
        }

        /// <summary>
        /// The name of the topic.
        /// </summary>
        public string Topic { get; set; }
        /// <summary>
        /// The id of the partition the fetch is for.
        /// </summary>
        public int PartitionId { get; set; }
        /// <summary>
        /// The offset to begin this fetch from.
        /// </summary>
        public long Offset { get; set; }
        /// <summary>
        /// The maximum bytes to include in the message set for this partition. This helps bound the size of the response.
        /// </summary>
        public int MaxBytes { get; set; }
    }

    public class FetchResponse
    {
        /// <summary>
        /// The name of the topic this response entry is for.
        /// </summary>
        public string Topic { get; set; }
        /// <summary>
        /// The id of the partition this response is for.
        /// </summary>
        public int PartitionId { get; set; }
        /// <summary>
        /// Error code of exception that occured during the request.  Zero if no error.
        /// </summary>
        public Int16 Error { get; set; }
        /// <summary>
        /// The offset at the end of the log for this partition. This can be used by the client to determine how many messages behind the end of the log they are.
        /// </summary>
        public long HighWaterMark { get; set; }

        public List<Message> Messages { get; set; }

        public FetchResponse()
        {
            Messages = new List<Message>();
        }
    }
}