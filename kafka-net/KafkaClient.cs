using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet.Model;
using KafkaNet.Common;


namespace KafkaNet
{
    /// <summary>
    /// TODO: need to put in place a way to record and log errors.
    /// </summary>
    public class KafkaClient : IDisposable
    {
        private readonly KafkaClientOptions _kafkaOptions;
        private readonly KafkaRouter _router;
        

        public KafkaClient(KafkaClientOptions kafkaOptions)
        {
            _kafkaOptions = kafkaOptions;
            _router = new KafkaRouter(kafkaOptions);
        }

        public Task<List<ProduceResponse>> SendAsync(ProduceRequest request)
        {
            request.CorrelationId = NextCorrelationId();

            //fire and forget, no response required
            if (request.Acks == 0)
            {
                var tcs = new TaskCompletionSource<List<ProduceResponse>>();
                _conn.SendAsync(_protocol.EncodeProduceRequest(request));
                tcs.SetResult(new List<ProduceResponse>());
                return tcs.Task;
            }

            //register callback for response
            var responseCallback = RegisterAsyncResponse(request, x => _protocol.DecodeProduceResponse(x).ToList());

            //send the request
            _conn.SendAsync(_protocol.EncodeProduceRequest(request));

            //return the response callback task
            return responseCallback;
        }

        public Task<MetadataResponse> SendAsync(MetadataRequest request)
        {
            request.CorrelationId = NextCorrelationId();

            var responseCallback = RegisterAsyncResponse(request, _protocol.DecodeMetadataResponse);
            _conn.SendAsync(_protocol.EncodeMetadataRequest(request));
            return responseCallback;
        }

        public Task<List<FetchResponse>> SendAsync(FetchRequest request)
        {
            request.CorrelationId = NextCorrelationId();

            var responseCallback = RegisterAsyncResponse(request, x => _protocol.DecodeFetchResponse(x).ToList());
            _conn.SendAsync(_protocol.EncodeFetchRequest(request));
            return responseCallback;
        }

        public Task<List<OffsetResponse>> SendAsync(OffsetRequest request)
        {
            request.CorrelationId = NextCorrelationId();

            var responseCallback = RegisterAsyncResponse(request, x => _protocol.DecodeOffsetResponse(x).ToList());
            _conn.SendAsync(_protocol.EncodeOffsetRequest(request));
            return responseCallback;
        }

        public Task<List<OffsetCommitResponse>> SendAsync(OffsetCommitRequest request)
        {
            throw new NotSupportedException("Not currently supported in kafka version 0.8.  https://issues.apache.org/jira/browse/KAFKA-993");
            //var response = await _conn.SendReceiveAsync(_protocol.EncodeOffsetCommitRequest(request));
            //return _protocol.DecodeOffsetCommitResponse(response).ToList();
        }

     
     


        public void Dispose()
        {
            using(_responseTimeoutTimer)
            using (_conn)
            {

            }
        }
    }

}
