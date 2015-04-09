using System.Collections.Generic;
using SimpleKafka.Common;

namespace SimpleKafka.Protocol
{
    public class MetadataRequest : BaseRequest, IKafkaRequest<MetadataResponse>
    {
        /// <summary>
        /// Indicates the type of kafka encoding this request is
        /// </summary>
        public ApiKeyRequestType ApiKey { get { return ApiKeyRequestType.MetaData; } }

        /// <summary>
        /// The list of topics to get metadata for.
        /// </summary>
        public List<string> Topics { get; set; }

        public void Encode(ref BigEndianEncoder encoder)
        {
            EncodeMetadataRequest(this, ref encoder);
        }

        public MetadataResponse Decode(ref BigEndianDecoder decoder)
        {
            return DecodeMetadataResponse(ref decoder);
        }

        private static void EncodeMetadataRequest(MetadataRequest request, ref BigEndianEncoder encoder)
        {
            if (request.Topics == null) request.Topics = new List<string>();
            EncodeHeader(request, ref encoder);
            encoder.Write(request.Topics.Count);
            foreach (var topic in request.Topics)
            {
                encoder.Write(topic, StringPrefixEncoding.Int16);
            }
        }

        private static MetadataResponse DecodeMetadataResponse(ref BigEndianDecoder decoder)
        {
            var response = new MetadataResponse
            {
                CorrelationId = decoder.ReadInt32(),
            };

            var brokerCount = decoder.ReadInt32();
            var brokers = new List<Broker>(brokerCount);
            for (var i = 0; i < brokerCount; i++)
            {
                brokers.Add(Broker.Decode(ref decoder));
            }
            response.Brokers = brokers;

            var topicCount = decoder.ReadInt32();
            var topics = new List<Topic>(topicCount);
            for (var i = 0; i < topicCount; i++)
            {
                topics.Add(Topic.Decode(ref decoder));
            }
            response.Topics = topics;

            return response;
        }

    }

    public class MetadataResponse
    {
        public int CorrelationId { get; set; }
        public MetadataResponse()
        {
            Brokers = new List<Broker>();
            Topics = new List<Topic>();
        }

        public List<Broker> Brokers { get; set; }
        public List<Topic> Topics { get; set; }
    }
}