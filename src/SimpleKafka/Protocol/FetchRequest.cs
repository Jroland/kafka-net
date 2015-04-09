using System;
using System.Collections.Generic;
using System.Linq;
using SimpleKafka.Common;

namespace SimpleKafka.Protocol
{
    public class FetchRequest : BaseRequest, IKafkaRequest<List<FetchResponse>>
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

        public void Encode(ref BigEndianEncoder encoder)
        {
            EncodeFetchRequest(this, ref encoder);
        }

        public List<FetchResponse> Decode(ref BigEndianDecoder decoder)
        {
            return DecodeFetchResponses(ref decoder);
        }

        private static void EncodeFetchRequest(FetchRequest request, ref BigEndianEncoder encoder)
        {
            if (request.Fetches == null) request.Fetches = new List<Fetch>();
            EncodeHeader(request, ref encoder);

            var topicGroups = request.Fetches.GroupBy(x => x.Topic).ToList();
            encoder.Write(ReplicaId);
            encoder.Write(request.MaxWaitTime);
            encoder.Write(request.MinBytes);
            encoder.Write(topicGroups.Count);

            foreach (var topicGroup in topicGroups)
            {
                var partitions = topicGroup.GroupBy(x => x.PartitionId).ToList();
                encoder.Write(topicGroup.Key, StringPrefixEncoding.Int16);
                encoder.Write(partitions.Count);

                foreach (var partition in partitions)
                {
                    foreach (var fetch in partition)
                    {
                        encoder.Write(partition.Key);
                        encoder.Write(fetch.Offset);
                        encoder.Write(fetch.MaxBytes);
                    }
                }
            }
        }

        private List<FetchResponse> DecodeFetchResponses(ref BigEndianDecoder decoder)
        {
            var correlationId = decoder.ReadInt32();

            var result = new List<FetchResponse>();

            var topicCount = decoder.ReadInt32();
            for (int i = 0; i < topicCount; i++)
            {
                var topic = decoder.ReadInt16String();

                var partitionCount = decoder.ReadInt32();
                for (int j = 0; j < partitionCount; j++)
                {
                    var partitionId = decoder.ReadInt32();
                    var response = new FetchResponse
                    {
                        Topic = topic,
                        PartitionId = partitionId,
                        Error = decoder.ReadInt16(),
                        HighWaterMark = decoder.ReadInt64(),
                    };
                    var messageSetSize = decoder.ReadInt32();
                    var current = decoder.Offset;
                    response.Messages = Message.DecodeMessageSet(partitionId, ref decoder, messageSetSize);
                    result.Add(response);

                    // In case any truncated messages
                    decoder.SetOffset(current + messageSetSize);
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