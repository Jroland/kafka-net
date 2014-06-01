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
    /// <summary>
    /// The TcpSocket provides an abstraction from the main driver from having to handle connection to and reconnections with a server.
    /// The interface is intentionally limited to only read/write.  All connection and reconnect details are handled internally.
    /// </summary>
    public class KafkaTcpSocket : IKafkaTcpSocket
    {
        public event ReconnectionAttempDelegate OnReconnectionAttempt;
        public delegate void ReconnectionAttempDelegate(int attempt);

        private const int DefaultReconnectionTimeout = 500;
        private const int DefaultReconnectionTimeoutMultiplier = 2;

        private readonly SemaphoreSlim _readSemaphore = new SemaphoreSlim(1,1);
        private readonly ThreadWall _threadWall = new ThreadWall(ThreadWallInitialState.Blocked);
        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private readonly IKafkaLog _log;
        private readonly Uri _serverUri;
        
        private TcpClient _client;
        private int _ensureOneThread;

        /// <summary>
        /// Construct socket and open connection to a specified server.
        /// </summary>
        /// <param name="log">Logging facility for verbose messaging of actions.</param>
        /// <param name="serverUri">The server to connect to.</param>
        /// <param name="delayConnectAttemptMS">Time in milliseconds to delay the initial connection attempt to the given server.</param>
        public KafkaTcpSocket(IKafkaLog log, Uri serverUri, int delayConnectAttemptMS = 0)
        {
            _log = log;
            _serverUri = serverUri;
            Task.Delay(TimeSpan.FromMilliseconds(delayConnectAttemptMS)).ContinueWith(x => TriggerReconnection());
        }

        #region Interface Implementation...
        /// <summary>
        /// The Uri to the connected server.
        /// </summary>
        public Uri ClientUri { get { return _serverUri; } }

        /// <summary>
        /// Read a certain byte array size return only when all bytes received.
        /// </summary>
        /// <param name="readSize">The size in bytes to receive from server.</param>
        /// <returns>Returns a byte[] array with the size of readSize.</returns>
        public Task<byte[]> ReadAsync(int readSize)
        {
            return EnsureReadAsync(readSize, _disposeToken.Token);
        }

        /// <summary>
        /// Read a certain byte array size return only when all bytes received.
        /// </summary>
        /// <param name="readSize">The size in bytes to receive from server.</param>
        /// <param name="cancellationToken">A cancellation token which will cancel the request.</param>
        /// <returns>Returns a byte[] array with the size of readSize.</returns>
        public Task<byte[]> ReadAsync(int readSize, CancellationToken cancellationToken)
        {
            return EnsureReadAsync(readSize, cancellationToken);
        }

        /// <summary>
        /// Convenience function to write full buffer data to the server.
        /// </summary>
        /// <param name="buffer">The buffer data to send.</param>
        /// <returns>Returns Task handle to the write operation.</returns>
        public Task WriteAsync(byte[] buffer)
        {
            return WriteAsync(buffer, 0, buffer.Length, _disposeToken.Token);
        }

        /// <summary>
        /// Write the buffer data to the server.
        /// </summary>
        /// <param name="buffer">The buffer data to send.</param>
        /// <param name="offset">The offset to start the read from the buffer.</param>
        /// <param name="count">The length of data to read off the buffer.</param>
        /// <returns>Returns Task handle to the write operation.</returns>
        public Task WriteAsync(byte[] buffer, int offset, int count)
        {
            return WriteAsync(buffer, offset, count, _disposeToken.Token);
        }

        /// <summary>
        /// Write the buffer data to the server.
        /// </summary>
        /// <param name="buffer">The buffer data to send.</param>
        /// <param name="offset">The offset to start the read from the buffer.</param>
        /// <param name="count">The length of data to read off the buffer.</param>
        /// <param name="cancellationToken">A cancellation token which will cancel the request.</param>
        /// <returns>Returns Task handle to the write operation.</returns>
        public Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return EnsureWriteAsync(buffer, offset, count, cancellationToken);
        }
        #endregion

        private Task<TcpClient> GetClientAsync()
        {
            return _threadWall.RequestPassageAsync().ContinueWith(t =>
                {
                    if (_client == null) 
                        throw new ServerUnreachableException(string.Format("Connection to {0} was not established.", _serverUri));
                    return _client;
                });
        }

        private async Task EnsureWriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            try
            {
                var client = await GetClientAsync();
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
                await _readSemaphore.WaitAsync();
                var cancelTask = new TaskCompletionSource<byte>();
                token.Register(cancelTask.SetCanceled);

                var result = new List<byte>();
                var bytesReceived = 0;

                while (bytesReceived < readSize)
                {
                    readSize = readSize - bytesReceived;
                    var buffer = new byte[readSize];

                    var client = await GetClientAsync();
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
            finally
            {
                _readSemaphore.Release();
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
            _log.WarnFormat("No connection to:{0}.  Attempting to re-connect...", ClientUri);

            //clean up existing client
            using (_client) { }

            while (_disposeToken.IsCancellationRequested == false)
            {
                try
                {
                    if (OnReconnectionAttempt != null) OnReconnectionAttempt(attempts++);
                    _client = new TcpClient();
                    await _client.ConnectAsync(_serverUri.Host, _serverUri.Port);
                    _log.WarnFormat("Connection established to:{0}.", ClientUri);
                    return _client;
                }
                catch
                {
                    reconnectionDelay = reconnectionDelay * DefaultReconnectionTimeoutMultiplier;
                    _log.WarnFormat("Failed re-connection to:{0}.  Will retry in:{1}", ClientUri, reconnectionDelay);
                    Task.Delay(TimeSpan.FromMilliseconds(reconnectionDelay)).Wait();
                }
            }

            return _client;
        }

        public void Dispose()
        {
            _disposeToken.Cancel();

            using (_client)
            {
                
            }
        }
    }
}
