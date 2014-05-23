using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using KafkaNet.Common;
using System.Threading;

namespace KafkaNet
{
    public class TcpSocket : ITcpSocket
    {
        private readonly object _threadLock = new object();
        private CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private TcpClient _client;
        private Uri _serverUri;

        public Uri ClientUri { get { return _serverUri; } }

        public TcpSocket(Uri serverUri)
        {
            _serverUri = serverUri;
        }

        public Task<byte[]> ReadAsync(int readSize)
        {
            return GetClient().GetStream().ReadAsync(readSize, _disposeToken.Token);
        }
        public Task<byte[]> ReadAsync(int readSize, System.Threading.CancellationToken cancellationToken)
        {
            return GetClient().GetStream().ReadAsync(readSize, cancellationToken);
        }

        public Task WriteAsync(byte[] buffer, int offset, int count)
        {
            return GetClient().GetStream().WriteAsync(buffer, offset, count, _disposeToken.Token);
        }
        public Task WriteAsync(byte[] buffer, int offset, int count, System.Threading.CancellationToken cancellationToken)
        {
            return GetClient().GetStream().WriteAsync(buffer, offset, count, cancellationToken);
        }

        private TcpClient GetClient()
        {
            if (_client == null || _client.Connected == false)
            {
                lock (_threadLock)
                {
                    if (_client == null || _client.Connected == false)
                    {
                        _client = new TcpClient();
                        _client.Connect(_serverUri.Host, _serverUri.Port);
                    }
                }
            }
            return _client;
        }

        public void Dispose()
        {
            using (_client)
            using (_disposeToken)
            {
                _disposeToken.Cancel();
            }
        }
    }

    public static class NetworkStreamExtensions
    {
        public static Task<byte[]> ReadAsync(this NetworkStream stream, int readSize)
        {
            return ReadAsync(stream, readSize, CancellationToken.None);
        }

        public static async Task<byte[]> ReadAsync(this NetworkStream stream, int readSize, CancellationToken token)
        {
            var buffer = new byte[readSize];
            await stream.ReadAsync(buffer, 0, readSize, token);
            return buffer;
        }
    }
}
