using System;
using System.Collections.Generic;
using SimpleKafka.Common;

namespace SimpleKafka.Protocol
{
    /// <summary>
    /// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetFetchRequest
    /// The offsets for a given consumer group is maintained by a specific broker called the offset coordinator. i.e., a consumer needs 
    /// to issue its offset commit and fetch requests to this specific broker. It can discover the current offset coordinator by issuing a consumer metadata request.
    /// </summary>
    public class ConsumerMetadataRequest : BaseRequest, IKafkaRequest<ConsumerMetadataResponse>
    {
        public ApiKeyRequestType ApiKey { get { return ApiKeyRequestType.ConsumerMetadataRequest; } }
        public string ConsumerGroup { get; set; }

        public void Encode(ref KafkaEncoder encoder)
        {
            EncodeConsumerMetadataRequest(this, ref encoder);
        }


        public ConsumerMetadataResponse Decode(ref KafkaDecoder decoder)
        {
            return DecodeConsumerMetadataResponse(ref decoder);
        }

        private static void EncodeConsumerMetadataRequest(ConsumerMetadataRequest request, ref KafkaEncoder encoder)
        {
            EncodeHeader(request, ref encoder);
            encoder.Write(request.ConsumerGroup, StringPrefixEncoding.Int16);
        }

        private static ConsumerMetadataResponse DecodeConsumerMetadataResponse(ref KafkaDecoder decoder)
        {
            var correlationId = decoder.ReadInt32();
            return new ConsumerMetadataResponse(ref decoder);
        }
    }

    public class ConsumerMetadataResponse
    {
        /// <summary>
        /// Error code of exception that occured during the request.  Zero if no error.
        /// </summary>
        public readonly ErrorResponseCode Error;

        public readonly int CoordinatorId;
        public readonly string CoordinatorHost;
        public readonly int CoordinatorPort;

        internal ConsumerMetadataResponse(ref KafkaDecoder decoder)
        {
            Error = (ErrorResponseCode)decoder.ReadInt16();
            CoordinatorId = decoder.ReadInt32();
            CoordinatorHost = decoder.ReadInt16String();
            CoordinatorPort = decoder.ReadInt32();
        }
    }
}
