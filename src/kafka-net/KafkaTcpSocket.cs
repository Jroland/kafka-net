using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.Threading;

namespace KafkaNet
{
    public class KafkaTcpSocket : IKafkaTcpSocket
    {
        private readonly object _threadLock = new object();
        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private readonly Uri _serverUri;

        private TcpClient _client;

        public Uri ClientUri { get { return _serverUri; } }

        public KafkaTcpSocket(Uri serverUri)
        {
            _serverUri = serverUri;
        }

        public Task<byte[]> ReadAsync(int readSize)
        {
            return GetClient().GetStream().ReadAsync(readSize, _disposeToken.Token);
        }
        public Task<byte[]> ReadAsync(int readSize, CancellationToken cancellationToken)
        {
            return GetClient().GetStream().ReadAsync(readSize, cancellationToken);
        }

        public Task WriteAsync(byte[] buffer, int offset, int count)
        {
            return GetClient().GetStream().WriteAsync(buffer, offset, count, _disposeToken.Token);
        }
        public Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
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
            var cancelTask = new TaskCompletionSource<byte>();
            token.Register(cancelTask.SetCanceled);

            var result = new List<byte>();
            var bytesReceived = 0;

            while (bytesReceived < readSize)
            {
                readSize = readSize - bytesReceived;
                var buffer = new byte[readSize];

                var readTask = stream.ReadAsync(buffer, 0, readSize, token);
                var completedTask = await Task.WhenAny(cancelTask.Task, readTask);

                if (completedTask == cancelTask.Task)
                {
                    throw new TaskCanceledException("Task cancel token was set.");
                }

                bytesReceived = readTask.Result;
                result.AddRange(buffer.Take(bytesReceived));
            }
            return result.ToArray();
        }
    }
}
