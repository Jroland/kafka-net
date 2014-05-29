using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.Threading;
using KafkaNet.Common;
using KafkaNet.Protocol;

namespace KafkaNet
{
    public class KafkaTcpSocket : IKafkaTcpSocket
    {
        public event ReconnectionAttempDelegate OnReconnectionAttempt;
        public delegate void ReconnectionAttempDelegate(int attempt);

        private const int DefaultReconnectionTimeout = 500;
        private const int DefaultReconnectionTimeoutMultiplier = 2;

        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private readonly Uri _serverUri;
        private readonly int _delayConnectionMS;
        private readonly IKafkaLog log = new DefaultTraceLog(); //TODO inject this

        private readonly ThreadWall _threadWall = new ThreadWall();
        private TcpClient _client;
        private int _ensureOneThread;

        public Uri ClientUri { get { return _serverUri; } }

        public KafkaTcpSocket(Uri serverUri, int delayInitialConnectionMS = 0)
        {
            _serverUri = serverUri;
            _delayConnectionMS = delayInitialConnectionMS;
            Task.Delay(TimeSpan.FromMilliseconds(_delayConnectionMS)).ContinueWith(x => TriggerReconnection());
        }

        public Task<byte[]> ReadAsync(int readSize)
        {
            return EnsureReadAsync(readSize, _disposeToken.Token);
        }
        public Task<byte[]> ReadAsync(int readSize, CancellationToken cancellationToken)
        {
            return EnsureReadAsync(readSize, cancellationToken);
        }

        public Task WriteAsync(byte[] buffer, int offset, int count)
        {
            return WriteAsync(buffer, offset, count, _disposeToken.Token);
        }
        public Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return EnsureWriteAsync(buffer, offset, count, cancellationToken);
        }

        private Task<TcpClient> GetClientAsync()
        {
            return _threadWall.RequestPassageAsync().ContinueWith(t => _client);
        }

        private async Task EnsureWriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            try
            {
                var client = await GetClientAsync();
                if (client == null) throw new ServerDisconnectedException(string.Format("Lost connection to server: {0}", _serverUri));
                await client.GetStream().WriteAsync(buffer, offset, count, cancellationToken);
            }
            catch
            {
                if (_disposeToken.IsCancellationRequested) throw new ObjectDisposedException("Object is disposing.");
                throw;
            }
        }

        private async Task<byte[]> EnsureReadAsync(int readSize, CancellationToken token)
        {
            try
            {
                var cancelTask = new TaskCompletionSource<byte>();
                token.Register(cancelTask.SetCanceled);

                var result = new List<byte>();
                var bytesReceived = 0;

                while (bytesReceived < readSize)
                {
                    readSize = readSize - bytesReceived;
                    var buffer = new byte[readSize];

                    var client = await GetClientAsync();
                    if (client == null) throw new ServerDisconnectedException(string.Format("Lost connection to server: {0}", _serverUri));
                    var readTask = client.GetStream().ReadAsync(buffer, 0, readSize, token);
                    var completedTask = await Task.WhenAny(cancelTask.Task, readTask);

                    if (completedTask == cancelTask.Task)
                    {
                        throw new TaskCanceledException("Task cancel token was set.");
                    }

                    bytesReceived = readTask.Result;
                    if (bytesReceived <= 0)
                    {
                        TriggerReconnection();
                        throw new ServerDisconnectedException(string.Format("Lost connection to server: {0}", _serverUri));
                    }

                    result.AddRange(buffer.Take(bytesReceived));
                }

                return result.ToArray();
            }
            catch
            {
                if (_disposeToken.IsCancellationRequested) throw new ObjectDisposedException("Object is disposing.");
                throw;
            }
        }

        private void TriggerReconnection()
        {
            //only allow one thread to trigger a reconnection, all other requests should be ignored.
            if (Interlocked.Increment(ref _ensureOneThread) == 1)
            {
                _threadWall.Block();
                ReEstablishConnection().ContinueWith(x =>
                {
                    Interlocked.Decrement(ref _ensureOneThread);
                    _threadWall.Release();
                });
            }
            else
            {
                Interlocked.Decrement(ref _ensureOneThread);
            }
        }

        private async Task<TcpClient> ReEstablishConnection()
        {
            var attempts = 1;
            var reconnectionDelay = DefaultReconnectionTimeout;
            log.WarnFormat("No connection to:{0}.  Attempting to re-connect...", ClientUri);

            //clean up existing client
            using (_client) { }

            while (_disposeToken.IsCancellationRequested == false)
            {
                try
                {
                    if (OnReconnectionAttempt != null) OnReconnectionAttempt(attempts++);
                    _client = new TcpClient();
                    await _client.ConnectAsync(_serverUri.Host, _serverUri.Port);
                    log.WarnFormat("Connection established to:{0}.", ClientUri);
                    return _client;
                }
                catch
                {
                    reconnectionDelay = reconnectionDelay * DefaultReconnectionTimeoutMultiplier;
                    log.WarnFormat("Failed re-connection to:{0}.  Will retry in:{1}", ClientUri, reconnectionDelay);
                    Task.Delay(TimeSpan.FromMilliseconds(reconnectionDelay)).Wait();
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
}
