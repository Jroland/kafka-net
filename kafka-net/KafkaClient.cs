using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kafka.Model;

namespace Kafka
{
    public class KafkaClient
    {
        private readonly KafkaConnection _conn;
        private readonly Protocol _protocol;

        public KafkaClient(Uri connection)
        {
            _conn = new KafkaConnection(connection);
            _protocol = new Protocol();
        }

        public async Task<List<ProduceResponse>> SendAsync(ProduceRequest request)
        {
            if (request.Acks == 0)
            {
                await _conn.SendAsync(_protocol.EncodeProduceRequest(request));
                return new List<ProduceResponse>();
            }
            
            var response = await _conn.SendReceiveAsync(_protocol.EncodeProduceRequest(request));
            return _protocol.DecodeProduceResponse(response).ToList();
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

        public async Task<List<OffsetResponse>> SendAsync(OffsetRequest request)
        {
            var response = await _conn.SendReceiveAsync(_protocol.EncodeOffsetRequest(request));
            return _protocol.DecodeOffsetResponse(response).ToList();
        }

        public async Task<List<OffsetCommitResponse>> SendAsync(OffsetCommitRequest request)
        {
            throw new NotSupportedException("Not currently supported in kafka version 0.8.  https://issues.apache.org/jira/browse/KAFKA-993");
            var response = await _conn.SendReceiveAsync(_protocol.EncodeOffsetCommitRequest(request));
            return _protocol.DecodeOffsetCommitResponse(response).ToList();
        }
    }
}
