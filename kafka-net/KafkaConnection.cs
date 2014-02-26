using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet.Common;

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
        private const int DefaultResponseTimeoutMs = 30000;

        private readonly object _threadLock = new object();
        private readonly ConcurrentDictionary<int, AsyncRequestItem> _requestIndex = new ConcurrentDictionary<int, AsyncRequestItem>();
        private readonly IScheduledTimer _responseTimeoutTimer;
        private readonly int _responseTimeoutMS;
        private readonly IKafkaLog _log;
        private readonly Uri _kafkaUri;
        private TcpClient _client;
        private bool _interrupt;
        private int _readerActive;
        private int _correlationIdSeed;

        /// <summary>
        /// Initializes a new instance of the KafkaConnection class.
        /// </summary>
        /// <param name="log">Logging interface used to record any log messages created by the connection.</param>
        /// <param name="serverAddress">The Uri address to this kafka server.</param>
        /// <param name="responseTimeoutMs">The amount of time to wait for a message response to be received from kafka.</param>
        public KafkaConnection(Uri serverAddress, int responseTimeoutMs = DefaultResponseTimeoutMs, IKafkaLog log = null)
        {
            _log = log ?? new DefaultTraceLog();
            _kafkaUri = serverAddress;
            _responseTimeoutMS = responseTimeoutMs;
            _responseTimeoutTimer = new ScheduledTimer()
                .Do(ResponseTimeoutCheck)
                .Every(TimeSpan.FromMilliseconds(100))
                .StartingAt(DateTime.Now.AddMilliseconds(_responseTimeoutMS))
                .Begin();
        }

        /// <summary>
        /// Indicates a thread is polling the stream for data to read.
        /// </summary>
        public bool ReadPolling
        {
            get { return _readerActive >= 1; }
        }

        /// <summary>
        /// Uri connection to kafka server.
        /// </summary>
        public Uri KafkaUri
        {
            get { return _kafkaUri; }
        }

        /// <summary>
        /// Send raw byte[] payload to the kafka server with a task indicating upload is complete.
        /// </summary>
        /// <param name="payload">kafka protocol formatted byte[] payload</param>
        /// <returns>Task which signals the completion of the upload of data to the server.</returns>
        public Task SendAsync(byte[] payload)
        {
            return GetStream().WriteAsync(payload, 0, payload.Length);
        }


        /// <summary>
        /// Send kafka payload to server and receive a task event when response is received.
        /// </summary>
        /// <typeparam name="T">A Kafka response object return by decode function.</typeparam>
        /// <param name="request">The IKafkaRequest to send to the kafka servers.</param>
        /// <returns></returns>
        public Task<List<T>> SendAsync<T>(IKafkaRequest<T> request)
        {
            //assign unique correlationId
            request.CorrelationId = NextCorrelationId();

            var tcs = new TaskCompletionSource<List<T>>();
            var asynRequest = new AsyncRequestItem(request.CorrelationId);
            asynRequest.ReceiveTask.Task.ContinueWith(data =>
            {
                try
                {
                    var response = request.Decode(data.Result);
                    tcs.SetResult(response.ToList());
                }
                catch (Exception ex)
                {
                    tcs.SetException(ex);
                }
            });

            if (_requestIndex.TryAdd(request.CorrelationId, asynRequest) == false)
                throw new ApplicationException("Failed to register request for async response.");

            SendAsync(request.Encode());

            return tcs.Task;
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
            return Equals(_kafkaUri, other._kafkaUri);
        }

        public override int GetHashCode()
        {
            return (_kafkaUri != null ? _kafkaUri.GetHashCode() : 0);
        } 
        #endregion

        private byte[] Read(int size, NetworkStream stream)
        {
            var buffer = new byte[size];
            stream.Read(buffer, 0, size);
            return buffer;
        }

        private TcpClient GetClient()
        {
            if (_client == null || _client.Connected == false || ReadPolling == false)
            {
                lock (_threadLock)
                {
                    if (_client == null || _client.Connected == false)
                    {
                        _client = new TcpClient();
                        _client.Connect(_kafkaUri.Host, _kafkaUri.Port);
                    }

                    if (ReadPolling == false) StartReadSteamPoller();
                }
            }
            return _client;
        }

        private NetworkStream GetStream()
        {
            var client = GetClient();
            return client.GetStream();
        }

        private void StartReadSteamPoller()
        {
            //This thread will poll the receive stream for data, parce a message out
            //and trigger an event with the message payload
            Task.Factory.StartNew(() =>
                {
                    while (_interrupt == false)
                    {
                        try
                        {
                            //only allow one reader to execute
                            if (Interlocked.Increment(ref _readerActive) > 1) return;

                            var stream = GetStream();
                            while (_interrupt == false)
                            {
                                while (stream.DataAvailable)
                                {
                                    //get message size
                                    var size = Read(4, stream).ToInt32();

                                    //load message and fire event with payload
                                    CorrelatePayloadToRequest(Read(size, stream));
                                }

                                Thread.Sleep(100);
                            }
                        }
                        catch (Exception ex)
                        {
                            //TODO being in sync with the byte order on read is important.  What happens if this exception causes us to be out of sync?
                            //record exception and continue to scan for data.
                            _log.ErrorFormat("Exception occured in polling read thread.  Exception={0}", ex);
                        }
                        finally
                        {
                            Interlocked.Decrement(ref _readerActive);
                        }
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
                Interlocked.Add(ref _correlationIdSeed, -1 * id);
            }
            return id;
        }

        /// <summary>
        /// Iterates the waiting response index for any requests that should be timed out and marks as exception.
        /// </summary>
        private void ResponseTimeoutCheck()
        {
            var timeouts = _requestIndex.Values.Where(x => x.CreatedOn < DateTime.UtcNow.AddMinutes(-1)).ToList();

            foreach (var timeout in timeouts)
            {
                AsyncRequestItem request;
                if (_requestIndex.TryRemove(timeout.CorrelationId, out request))
                {
                    request.ReceiveTask.SetException(new ResponseTimeoutException(
                        string.Format("Timeout Expired. Client failed to receive a response from server after waiting {0}ms.", _responseTimeoutMS)));
                }
            }
        }

        public void Dispose()
        {
            using (_client)
            using (_client.GetStream())
            using (_responseTimeoutTimer)
            {
                _interrupt = true;
            }
        }

        #region Class AsyncRequestItem...
        class AsyncRequestItem
        {
            public AsyncRequestItem(int correlationId)
            {
                CorrelationId = correlationId;
                CreatedOn = DateTime.UtcNow;
                ReceiveTask = new TaskCompletionSource<byte[]>();
            }

            public int CorrelationId { get; private set; }
            public TaskCompletionSource<byte[]> ReceiveTask { get; private set; }
            public DateTime CreatedOn { get; private set; }
        }
        #endregion
    }

    
}
