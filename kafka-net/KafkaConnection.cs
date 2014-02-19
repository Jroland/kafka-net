using System;
using System.Net.Sockets;
using System.Threading.Tasks;
using KafkaNet.Common;

namespace KafkaNet
{
    /// <summary>
    /// TODO not currently thread safe
    /// </summary>
    public class KafkaConnection : IDisposable
    {
        private readonly Uri _kafkaUri;
        private readonly int _readTimeoutMS;
        private readonly TcpClient _client = new TcpClient();
        private bool _disposed;

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
        public async Task SendAsync(byte[] payload)
        {
            var stream = await GetStream();
            await stream.WriteAsync(payload, 0, payload.Length);
        }

        /// <summary>
        /// Read a response from kafka server
        /// </summary>
        /// <returns>Task handle with byte[] of response data from kafka.</returns>
        public async Task<byte[]> SendReceiveAsync(byte[] payload)
        {
            var stream = await GetStream();

            await stream.WriteAsync(payload, 0, payload.Length);

            //get message size from header
            var header = await ReadAsync(4, stream);

            var size = header.ToInt32();

            return await ReadAsync(size, stream);
        }

        private async Task<byte[]> ReadAsync(int size, NetworkStream stream)
        {
            var buffer = new byte[size];
            await stream.ReadAsync(buffer, 0, size);
            return buffer;
        }

        private async Task<TcpClient> GetClient()
        {
            if (_client.Connected == false)
            {
                await _client.ConnectAsync(_kafkaUri.Host, _kafkaUri.Port);
            }
            return _client;
        }

        private async Task<NetworkStream> GetStream()
        {
            var client = await GetClient();
            var stream = client.GetStream();
            stream.ReadTimeout = _readTimeoutMS;
            return stream;
        }

        public void Dispose()
        {
            using (_client)
            using (_client.GetStream())
            {
                _disposed = true;
            }
        }
    }
}
