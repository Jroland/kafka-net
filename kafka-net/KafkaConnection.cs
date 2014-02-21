using System;
using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet.Common;

namespace KafkaNet
{
    /// <summary>
    /// TODO not currently thread safe
    /// </summary>
    public class KafkaConnection : IDisposable
    {
        public delegate void ResponseReceived(byte[] payload);
        public event ResponseReceived OnReponseReceived;

        private readonly BlockingCollection<byte[]> _readQueue = new BlockingCollection<byte[]>(100);
        private readonly ConcurrentDictionary<int, TaskCompletionSource<byte[]>> _requestIndex = new ConcurrentDictionary<int, TaskCompletionSource<byte[]>>();
        private readonly object _threadLock = new object();
        private readonly Uri _kafkaUri;
        private readonly int _readTimeoutMS;
        private TcpClient _client;
        private bool _interrupt;
        private int _readerActive;

        /// <summary>
        /// Initializes a new instance of the KafkaConnection class.
        /// </summary>
        /// <param name="serverAddress"></param>
        /// <param name="readTimeoutMS">The timeout for read operations</param>
        public KafkaConnection(Uri serverAddress, int readTimeoutMS = 5000)
        {
            _kafkaUri = serverAddress;
            _readTimeoutMS = readTimeoutMS;
        }

        /// <summary>
        /// Indicates thread is polling the stream for data to read.
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
        /// Send payload to the kafka server
        /// </summary>
        /// <param name="payload">kafka protocol formatted byte[] payload</param>
        /// <returns>Task handle to send operation.</returns>
        public Task SendAsync(byte[] payload)
        {
            return GetStream().WriteAsync(payload, 0, payload.Length);
        }
        
        private byte[] Read(int size, NetworkStream stream)
        {
            var buffer = new byte[size];
            stream.Read(buffer, 0, size);
            return buffer;
        }
        
        private void StartReadPoller()
        {
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
                                if (stream.DataAvailable)
                                {
                                    //get message size
                                    var size = Read(4, stream).ToInt32();

                                    //load message place on return queue
                                    if (OnReponseReceived != null)
                                        OnReponseReceived(Read(size, stream));
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            //record exception and continue to scan for data
                        }
                        finally
                        {
                            Interlocked.Decrement(ref _readerActive);
                        }
                    }
                });
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

                    if (ReadPolling == false) StartReadPoller();
                }
            }
            return _client;
        }

        private NetworkStream GetStream()
        {
            var client = GetClient();
            var stream = client.GetStream();
            stream.ReadTimeout = _readTimeoutMS;
            return stream;
        }

        public void Dispose()
        {
            using (_client)
            using (_client.GetStream())
            {
                _interrupt = true;
            }
        }
    }
}
