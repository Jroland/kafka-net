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
    public class ConsumerMetadataRequest : BaseRequest<ConsumerMetadataResponse>, IKafkaRequest
    {
        public string ConsumerGroup { get; set; }

        public ConsumerMetadataRequest() : base(ApiKeyRequestType.ConsumerMetadataRequest) { }

        internal override KafkaEncoder Encode(KafkaEncoder encoder)
        {
            return EncodeConsumerMetadataRequest(this, encoder);
        }


        internal override ConsumerMetadataResponse Decode(KafkaDecoder decoder)
        {
            return DecodeConsumerMetadataResponse(decoder);
        }

        private static KafkaEncoder EncodeConsumerMetadataRequest(ConsumerMetadataRequest request, KafkaEncoder encoder)
        {
            return 
                request
                .EncodeHeader(encoder)
                .Write(request.ConsumerGroup);
        }

        private static ConsumerMetadataResponse DecodeConsumerMetadataResponse(KafkaDecoder decoder)
        {
            var correlationId = decoder.ReadInt32();
            return ConsumerMetadataResponse.Decode(decoder);
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

        private ConsumerMetadataResponse(ErrorResponseCode error, int coordinatorId, string coordinatorHost, int coordinatorPort)
        {
            this.Error = error;
            this.CoordinatorId = coordinatorId;
            this.CoordinatorHost = coordinatorHost;
            this.CoordinatorPort = coordinatorPort;
        }

        internal static ConsumerMetadataResponse Decode(KafkaDecoder decoder)
        {
            var error = decoder.ReadErrorResponseCode();
            var coordinatorId = decoder.ReadInt32();
            var coordinatorHost = decoder.ReadString();
            var coordinatorPort = decoder.ReadInt32();

            return new ConsumerMetadataResponse(error, coordinatorId, coordinatorHost, coordinatorPort);
        }
    }
}
