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
        private const int DefaultResonseTimeout = 5000;
        private readonly KafkaConnection _conn;
        private readonly Protocol _protocol;
        private readonly ConcurrentDictionary<int, AsyncRequest> _requestIndex = new ConcurrentDictionary<int, AsyncRequest>();
        
        private readonly Timer _indexTimeoutTimer;
        private readonly int _responseTimeoutMS;

        private int _correlationIdSeed;
        
        public KafkaClient(Uri connection, int resopnseTimeoutMS = DefaultResonseTimeout)
        {
            _conn = new KafkaConnection(connection);
            _conn.OnResponseReceived += OnResponseReceived;
            _protocol = new Protocol();
            _indexTimeoutTimer = new Timer(ResponseTimeoutCallback, null, TimeSpan.FromMilliseconds(resopnseTimeoutMS), TimeSpan.FromMilliseconds(100));
            _responseTimeoutMS = resopnseTimeoutMS;
        }

        public int ResponseTimeoutMS { get { return _responseTimeoutMS; } }

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

            var asynRequest = new AsyncRequest(request.CorrelationId);
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

        private void OnResponseReceived(byte[] payload)
        {
            try
            {
                var correlationId = payload.Take(4).ToArray().ToInt32();
                AsyncRequest asyncRequest;
                if (_requestIndex.TryRemove(correlationId, out asyncRequest))
                {
                    asyncRequest.TaskSource.SetResult(payload);
                }
                else
                {
                    //TODO received a message but failed to find it in the index
                }
            }
            catch (Exception ex)
            {
                //TODO record unexpected failure
            }
        }

        private int NextCorrelationId()
        {
            var id = Interlocked.Increment(ref _correlationIdSeed);
            if (id > int.MaxValue - 100) //somewhere close to max reset.
            {
                Interlocked.Add(ref _correlationIdSeed, -1 * id);
            }
            return id;
        }

        /// <summary>
        /// Iterates the waiting response index for any requests that should be timed out and marks as exception.
        /// </summary>
        private void ResponseTimeoutCallback(object state)
        {
            var timeouts = _requestIndex.Values.Where(x => x.CreatedOn < DateTime.UtcNow.AddMinutes(-1)).ToList();

            foreach (var timeout in timeouts)
            {
                AsyncRequest request;
                if (_requestIndex.TryRemove(timeout.CorrelationId, out request))
                {
                    request.TaskSource.SetException(new ResponseTimeoutException(
                        string.Format("Timeout Expired. Client failed to receive a response from server after waiting {0}ms.", _responseTimeoutMS)));
                }
            }
        }

        public void Dispose()
        {
            using(_indexTimeoutTimer)
            using (_conn)
            {

            }
        }
    }

    partial class AsyncRequest
    {
        public AsyncRequest(int correlationId)
        {
            CorrelationId = correlationId;
            CreatedOn = DateTime.UtcNow;
            TaskSource = new TaskCompletionSource<byte[]>();
        }

        public int CorrelationId { get; set; }
        public TaskCompletionSource<byte[]> TaskSource { get; set; }
        public DateTime CreatedOn { get; set; }
    }
}
