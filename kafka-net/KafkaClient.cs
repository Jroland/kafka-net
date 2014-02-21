using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet.Model;

namespace KafkaNet
{
    public class KafkaClient
    {
        private readonly KafkaConnection _conn;
        private readonly Protocol _protocol;
        private readonly ConcurrentDictionary<int, AsyncRequest> _requestIndex = new ConcurrentDictionary<int, AsyncRequest>();

        private int _correlationId;
        

        public KafkaClient(Uri connection)
        {
            _conn = new KafkaConnection(connection);
            _protocol = new Protocol();
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

        private Task<T> RegisterAsyncResponse<T>(IKafkaRequest request, Func<byte[], T> decodeFunction)
        {
            var tcs = new TaskCompletionSource<T>();

            var asynRequest = new AsyncRequest(request.CorrelationId, request.ApiKey);
            asynRequest.TaskSource.Task.ContinueWith(data =>
            {
                try
                {
                    var response = decodeFunction(data.Result);
                    tcs.SetResult(response);
                }
                catch (Exception ex)
                {
                    tcs.SetException(ex);
                }
            });

            if (_requestIndex.TryAdd(request.CorrelationId, asynRequest) == false)
                throw new ApplicationException("Failed to register request for async response.");

            return tcs.Task;
        }

        private int NextCorrelationId()
        {
            var id = Interlocked.Increment(ref _correlationId);
            if (id > int.MaxValue - 100) //somewhere close to max reset.
            {
                Interlocked.Add(ref _correlationId, -1 * id);
            }
            return id;
        }
    }

    class AsyncRequest
    {
        public AsyncRequest(int correlationId, ApiKeyRequestType apiKey)
        {
            DecodeKey = apiKey;
            CreatedOn = DateTime.UtcNow;
            TaskSource = new TaskCompletionSource<byte[]>();
        }

        public int CorrelationId { get; set; }
        public ApiKeyRequestType DecodeKey { get; set; }
        public TaskCompletionSource<byte[]> TaskSource { get; set; }
        public DateTime CreatedOn { get; set; }
    }
}
