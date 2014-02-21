using System;
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
    public class KafkaConnection : IDisposable
    {
        public delegate void ResponseReceived(byte[] payload);
        public event ResponseReceived OnResponseReceived;

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
        /// Send payload to the kafka server and complete task once payload is sent.
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
                                    if (OnResponseReceived != null) OnResponseReceived(Read(size, stream));
                                }
                                Thread.Sleep(100);
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
