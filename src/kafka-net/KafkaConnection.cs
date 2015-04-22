using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet.Common;
using KafkaNet.Model;
using KafkaNet.Protocol;

namespace KafkaNet
{
    /// <summary>
    /// KafkaConnection represents the lowest level TCP stream connection to a Kafka broker. 
    /// The Send and Receive are separated into two disconnected paths and must be combine outside
    /// this class by the correlation ID contained within the returned message.
    /// 
    /// The SendAsync function will return a Task and complete once the data has been sent to the outbound stream.
    /// The Read response is handled by a single thread polling the stream for data and firing an OnResponseReceived
    /// event when a response is received.
    /// </summary>
    public class KafkaConnection : IKafkaConnection
    {
        private const int DefaultResponseTimeoutMs = 60000;

        private readonly ConcurrentDictionary<int, AsyncRequestItem> _requestIndex = new ConcurrentDictionary<int, AsyncRequestItem>();
        private readonly TimeSpan _responseTimeoutMS;
        private readonly IKafkaLog _log;
        private readonly IKafkaTcpSocket _client;
        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();

        private int _disposeCount = 0;
        private Task _connectionReadPollingTask = null;
        private int _ensureOneActiveReader;
        private int _correlationIdSeed;

        /// <summary>
        /// Initializes a new instance of the KafkaConnection class.
        /// </summary>
        /// <param name="log">Logging interface used to record any log messages created by the connection.</param>
        /// <param name="client">The kafka socket initialized to the kafka server.</param>
        /// <param name="responseTimeoutMs">The amount of time to wait for a message response to be received after sending message to Kafka.  Defaults to 30s.</param>
        public KafkaConnection(IKafkaTcpSocket client, TimeSpan? responseTimeoutMs = null, IKafkaLog log = null)
        {
            _client = client;
            _log = log ?? new DefaultTraceLog();
            _responseTimeoutMS = responseTimeoutMs ?? TimeSpan.FromMilliseconds(DefaultResponseTimeoutMs);

            StartReadStreamPoller();
        }

        /// <summary>
        /// Indicates a thread is polling the stream for data to read.
        /// </summary>
        public bool ReadPolling
        {
            get { return _ensureOneActiveReader >= 1; }
        }

        /// <summary>
        /// Provides the unique ip/port endpoint for this connection
        /// </summary>
        public KafkaEndpoint Endpoint { get { return _client.Endpoint; } }

        /// <summary>
        /// Send raw byte[] payload to the kafka server with a task indicating upload is complete.
        /// </summary>
        /// <param name="payload">kafka protocol formatted byte[] payload</param>
        /// <returns>Task which signals the completion of the upload of data to the server.</returns>
        public Task SendAsync(KafkaDataPayload payload)
        {
            return _client.WriteAsync(payload);
        }

        /// <summary>
        /// Send raw byte[] payload to the kafka server with a task indicating upload is complete.
        /// </summary>
        /// <param name="payload">kafka protocol formatted byte[] payload</param>
        /// <param name="token">Cancellation token used to cancel the transfer.</param>
        /// <returns>Task which signals the completion of the upload of data to the server.</returns>
        public Task SendAsync(KafkaDataPayload payload, CancellationToken token)
        {
            return _client.WriteAsync(payload, token);
        }


        /// <summary>
        /// Send kafka payload to server and receive a task event when response is received.
        /// </summary>
        /// <typeparam name="T">A Kafka response object return by decode function.</typeparam>
        /// <param name="request">The IKafkaRequest to send to the kafka servers.</param>
        /// <returns></returns>
        public async Task<List<T>> SendAsync<T>(IKafkaRequest<T> request)
        {
            //assign unique correlationId
            request.CorrelationId = NextCorrelationId();

            //if response is expected, register a receive data task and send request
            if (request.ExpectResponse)
            {
                using (var asyncRequest = new AsyncRequestItem(request.CorrelationId))
                {

                    try
                    {
                        AddAsyncRequestItemToResponseQueue(asyncRequest);
                        await _client.WriteAsync(request.Encode())
                            .ContinueWith(t => asyncRequest.MarkRequestAsSent(t.Exception, _responseTimeoutMS, TriggerMessageTimeout))
                            .ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        TriggerMessageTimeout(asyncRequest);
                    }
                
                    var response = await asyncRequest.ReceiveTask.Task.ConfigureAwait(false);

                    return request.Decode(response).ToList();
                }
            }


            //no response needed, just send
            await _client.WriteAsync(request.Encode()).ConfigureAwait(false);
            //TODO should this return a response of success for request?
            return new List<T>();
        }

        #region Equals Override...
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((KafkaConnection)obj);
        }

        protected bool Equals(KafkaConnection other)
        {
            return Equals(_client.Endpoint, other.Endpoint);
        }

        public override int GetHashCode()
        {
            return (_client.Endpoint != null ? _client.Endpoint.GetHashCode() : 0);
        }
        #endregion

        private void StartReadStreamPoller()
        {
            //This thread will poll the receive stream for data, parce a message out
            //and trigger an event with the message payload
            _connectionReadPollingTask = Task.Run(async () =>
                {
                    try
                    {
                        //only allow one reader to execute, dump out all other requests
                        if (Interlocked.Increment(ref _ensureOneActiveReader) != 1) return;

                        while (_disposeToken.IsCancellationRequested == false)
                        {
                            try
                            {
                                _log.DebugFormat("Awaiting message from: {0}", _client.Endpoint);
                                var messageSizeResult = await _client.ReadAsync(4, _disposeToken.Token).ConfigureAwait(false);
                                var messageSize = messageSizeResult.ToInt32();

                                _log.DebugFormat("Received message of size: {0} From: {1}", messageSize, _client.Endpoint);
                                var message = await _client.ReadAsync(messageSize, _disposeToken.Token).ConfigureAwait(false);

                                CorrelatePayloadToRequest(message);
                            }
                            catch (Exception ex)
                            {
                                //don't record the exception if we are disposing
                                if (_disposeToken.IsCancellationRequested == false)
                                {
                                    //TODO being in sync with the byte order on read is important.  What happens if this exception causes us to be out of sync?
                                    //record exception and continue to scan for data.
                                    _log.ErrorFormat("Exception occured in polling read thread.  Exception={0}", ex);
                                }
                            }
                        }
                    }
                    finally
                    {
                        Interlocked.Decrement(ref _ensureOneActiveReader);
                        _log.DebugFormat("Closed down connection to: {0}", _client.Endpoint);
                    }
                });
        }

        private void CorrelatePayloadToRequest(byte[] payload)
        {
            var correlationId = payload.Take(4).ToArray().ToInt32();
            AsyncRequestItem asyncRequest;
            if (_requestIndex.TryRemove(correlationId, out asyncRequest))
            {
                asyncRequest.ReceiveTask.SetResult(payload);
            }
            else
            {
                _log.WarnFormat("Message response received with correlationId={0}, but did not exist in the request queue.", correlationId);
            }
        }

        private int NextCorrelationId()
        {
            var id = Interlocked.Increment(ref _correlationIdSeed);
            if (id > int.MaxValue - 100) //somewhere close to max reset.
            {
                Interlocked.Exchange(ref _correlationIdSeed, 0);
            }
            return id;
        }

        private void AddAsyncRequestItemToResponseQueue(AsyncRequestItem requestItem)
        {
            if (requestItem == null) return;
            if (_requestIndex.TryAdd(requestItem.CorrelationId, requestItem) == false)
                throw new ApplicationException("Failed to register request for async response.");
        }

        private void TriggerMessageTimeout(AsyncRequestItem asyncRequestItem)
        {
            if (asyncRequestItem == null) return;

            AsyncRequestItem request;
            _requestIndex.TryRemove(asyncRequestItem.CorrelationId, out request); //just remove it from the index

            if (_disposeToken.IsCancellationRequested)
            {
                asyncRequestItem.ReceiveTask.TrySetException(
                    new ObjectDisposedException("The object is being disposed and the connection is closing."));
            }
            else
            {
                asyncRequestItem.ReceiveTask.TrySetException(new ResponseTimeoutException(
                    string.Format("Timeout Expired. Client failed to receive a response from server after waiting {0}ms.",
                        _responseTimeoutMS)));
            }
        }

        public void Dispose()
        {
            //skip multiple calls to dispose
            if (Interlocked.Increment(ref _disposeCount) != 1) return;

            _disposeToken.Cancel();

            if (_connectionReadPollingTask != null) _connectionReadPollingTask.Wait(TimeSpan.FromSeconds(1));

            using (_disposeToken)
            using (_client)
            {

            }
        }

        #region Class AsyncRequestItem...
        class AsyncRequestItem : IDisposable
        {
            private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();

            public AsyncRequestItem(int correlationId)
            {
                CorrelationId = correlationId;
                ReceiveTask = new TaskCompletionSource<byte[]>();
            }

            public int CorrelationId { get; private set; }
            public TaskCompletionSource<byte[]> ReceiveTask { get; private set; }

            public void MarkRequestAsSent(Exception ex, TimeSpan timeout, Action<AsyncRequestItem> timeoutFunction)
            {
                if (ex != null)
                {
                    ReceiveTask.TrySetException(ex);
                    throw ex;
                }

                _cancellationTokenSource.CancelAfter(timeout);
                _cancellationTokenSource.Token.Register(() => timeoutFunction(this));
            }


            public void Dispose()
            {
                using (_cancellationTokenSource)
                {

                }
            }
        }
        #endregion
    }


}
