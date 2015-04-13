using System;
using System.Collections.Generic;
using System.Linq;
using SimpleKafka.Common;

namespace SimpleKafka.Protocol
{
    public class FetchRequest : BaseRequest<List<FetchResponse>>, IKafkaRequest
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

        public FetchRequest() : base(ApiKeyRequestType.Fetch) { }


        internal override KafkaEncoder Encode(KafkaEncoder encoder)
        {
            return EncodeFetchRequest(this, encoder);
        }

        internal override List<FetchResponse> Decode(KafkaDecoder decoder)
        {
            return DecodeFetchResponses(decoder);
        }

        private static KafkaEncoder EncodeFetchRequest(FetchRequest request, KafkaEncoder encoder)
        {
            request
                .EncodeHeader(encoder)
                .Write(ReplicaId)
                .Write(request.MaxWaitTime)
                .Write(request.MinBytes);

            if (request.Fetches == null)
            {
                // no topics
                encoder.Write(0);
            }
            else if (request.Fetches.Count == 1)
            {
                // single topic/partition - quick mode
                var fetch = request.Fetches[0];
                encoder
                    .Write(1)
                    .Write(fetch.Topic)
                    .Write(1);

                EncodeFetch(encoder, fetch);
            }
            else
            {
                // Multiple topics/partitions - slower mode
                var topicGroups = new Dictionary<string,List<Fetch>>();
                foreach (var fetch in request.Fetches) {
                    var fetchList = topicGroups.GetOrCreate(fetch.Topic, () => new List<Fetch>(request.Fetches.Count));
                    fetchList.Add(fetch);
                }

                encoder.Write(topicGroups.Count);
                foreach (var topicGroupKvp in topicGroups) {
                    var topicGroup = topicGroupKvp.Key;
                    var fetches = topicGroupKvp.Value;
                    encoder
                        .Write(topicGroup)
                        .Write(fetches.Count);
                    foreach (var fetch in fetches) {
                        EncodeFetch(encoder, fetch);
                    }
                }
            }
            return encoder;
        }

        private static void EncodeFetch(KafkaEncoder encoder, Fetch fetch)
        {
            encoder
                .Write(fetch.PartitionId)
                .Write(fetch.Offset)
                .Write(fetch.MaxBytes);
        }

        private List<FetchResponse> DecodeFetchResponses(KafkaDecoder decoder)
        {
            var correlationId = decoder.ReadInt32();

            var result = new List<FetchResponse>();

            var topicCount = decoder.ReadInt32();
            for (int i = 0; i < topicCount; i++)
            {
                var topic = decoder.ReadString();

                var partitionCount = decoder.ReadInt32();
                for (int j = 0; j < partitionCount; j++)
                {
                    var response = FetchResponse.Decode(decoder, topic);
                    result.Add(response);
                }
            }
            return result;
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
        public readonly string Topic;
        public readonly int PartitionId;
        public readonly ErrorResponseCode Error;
        public readonly long HighWaterMark;
        public readonly IList<Message> Messages;

        private FetchResponse(string topic, int partitionId, ErrorResponseCode error, long highWaterMark, IList<Message> messages)
        {
            this.Topic = topic;
            this.PartitionId = partitionId;
            this.Error = error;
            this.HighWaterMark = highWaterMark;
            this.Messages = messages;
        }

        internal static FetchResponse Decode(KafkaDecoder decoder, string topic)
        {
            var partitionId = decoder.ReadInt32();
            var error = decoder.ReadErrorResponseCode();
            var highWaterMark = decoder.ReadInt64();

            var messageSetSize = decoder.ReadInt32();
            var current = decoder.Offset;
            var messages = Message.DecodeMessageSet(partitionId, decoder, messageSetSize);
            var response = new FetchResponse(topic, partitionId, error, highWaterMark, messages);
            
            // In case any truncated messages
            decoder.SetOffset(current + messageSetSize);

            return response;
        }
    }
}