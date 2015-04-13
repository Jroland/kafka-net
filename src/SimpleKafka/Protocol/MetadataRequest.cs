using System.Collections.Generic;
using SimpleKafka.Common;

namespace SimpleKafka.Protocol
{
    public class MetadataRequest : BaseRequest<MetadataResponse>, IKafkaRequest
    {
        /// <summary>
        /// The list of topics to get metadata for.
        /// </summary>
        public List<string> Topics { get; set; }

        public MetadataRequest() : base(ApiKeyRequestType.MetaData) { }
        internal override KafkaEncoder Encode(KafkaEncoder encoder)
        {
            return EncodeMetadataRequest(this, encoder);
        }

        internal override MetadataResponse Decode(KafkaDecoder decoder)
        {
            return DecodeMetadataResponse(decoder);
        }

        private static KafkaEncoder EncodeMetadataRequest(MetadataRequest request, KafkaEncoder encoder)
        {
            request
                .EncodeHeader(encoder);

            if (request.Topics == null)
            {
                encoder.Write(0);
            }
            else
            {
                encoder.Write(request.Topics.Count);
                foreach (var topic in request.Topics)
                {
                    encoder.Write(topic);
                }
            }

            return encoder;
        }

        private static MetadataResponse DecodeMetadataResponse(KafkaDecoder decoder)
        {
            var correlationId = decoder.ReadInt32();
            var response = MetadataResponse.Decode(decoder);
            return response;
        }

    }

    public class MetadataResponse
    {
        public readonly Broker[] Brokers;
        public readonly Topic[] Topics;
        private MetadataResponse(Broker[] brokers, Topic[] topics)
        {
            this.Brokers = brokers;
            this.Topics = topics;
        }

        internal static MetadataResponse Decode(KafkaDecoder decoder)
        {
            var brokerCount = decoder.ReadInt32();
            var brokers = new Broker[brokerCount];
            for (var i = 0; i < brokerCount; i++)
            {
                brokers[i] = Broker.Decode(decoder);
            }

            var topicCount = decoder.ReadInt32();
            var topics = new Topic[topicCount];
            for (var i = 0; i < topicCount; i++)
            {
                topics[i] = Topic.Decode(decoder);
            }

            var response = new MetadataResponse(brokers, topics);

            return response;

        }
    }
}