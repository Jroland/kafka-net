using System.Collections.Generic;
using System.Linq;
using KafkaNet.Common;
using KafkaNet.Model;

namespace KafkaNet.Protocol
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

        public byte[] Encode()
        {
            return EncodeMetadataRequest(this);
        }

        public IEnumerable<MetadataResponse> Decode(byte[] payload)
        {
            return new[] { DecodeMetadataResponse(payload) };
        }

        /// <summary>
        /// Encode a request for metadata about topic and broker information.
        /// </summary>
        /// <param name="request">The MetaDataRequest to encode.</param>
        /// <returns>Encoded byte[] representing the request.</returns>
        /// <remarks>Format: (MessageSize), Header, ix(hs)</remarks>
        private byte[] EncodeMetadataRequest(MetadataRequest request)
        {
            var message = new WriteByteStream();

            if (request.Topics == null) request.Topics = new List<string>();

            message.Pack(EncodeHeader(request)); //header
            message.Pack(request.Topics.Count.ToBytes());
            message.Pack(request.Topics.Select(x => x.ToInt16SizedBytes()).ToArray());
            message.Prepend(message.Length().ToBytes());

            return message.Payload();
        }

        /// <summary>
        /// Decode the metadata response from kafka server.
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        private MetadataResponse DecodeMetadataResponse(byte[] data)
        {
            var stream = new ReadByteStream(data);
            var response = new MetadataResponse();
            response.CorrelationId = stream.ReadInt();

            var brokerCount = stream.ReadInt();
            for (var i = 0; i < brokerCount; i++)
            {
                response.Brokers.Add(Broker.FromStream(stream));
            }

            var topicCount = stream.ReadInt();
            for (var i = 0; i < topicCount; i++)
            {
                response.Topics.Add(Topic.FromStream(stream));
            }

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