using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Model;

namespace Kafka
{
    public class KafkaClient
    {
        private KafkaConnection _conn;
        private Protocol _protocol;

        public KafkaClient(Uri connection)
        {
            _conn = new KafkaConnection(connection);
            _protocol = new Protocol();
        }

        public void Send(ProduceRequest request)
        {
            _conn.SendAsync(_protocol.EncodeProduceRequest(request)).Wait();
        }

        public async Task<MetadataResponse> SendAsync(MetadataRequest request)
        {
            var response = await _conn.SendReceiveAsync(_protocol.EncodeMetadataRequest(request));
            return _protocol.DecodeMetadataResponse(response);
        }

        public async Task<List<FetchResponse>> SendAsync(FetchRequest request)
        {
            var response = await _conn.SendReceiveAsync(_protocol.EncodeFetchRequest(request));
            return _protocol.DecodeFetchResponse(response).ToList();
        }
    }
}
