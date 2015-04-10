using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.Threading;
using KafkaNet.Common;
using KafkaNet.Model;
using KafkaNet.Protocol;
using KafkaNet.Statistics;

namespace KafkaNet
{
    /// <summary>
    /// The TcpSocket provides an abstraction from the main driver from having to handle connection to and reconnections with a server.
    /// The interface is intentionally limited to only read/write.  All connection and reconnect details are handled internally.
    /// </summary>
    public class KafkaTcpSocket : IKafkaTcpSocket
    {
        public Action<int> OnReconnectionAttempt;

        private const int DefaultReconnectionTimeout = 500;
        private const int DefaultReconnectionTimeoutMultiplier = 2;
        private const int MaxReconnectionTimeoutMinutes = 5;

        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private readonly IKafkaLog _log;
        private readonly KafkaEndpoint _endpoint;
        private readonly TimeSpan _maximumReconnectionTimeout;

        private readonly AsyncLock _clientLock = new AsyncLock();
        private readonly AsyncLock _writeLock = new AsyncLock();
        private readonly AsyncLock _readLock = new AsyncLock();
        private TcpClient _client;
        private int _disposeCount;
        private readonly Task _clientConnectingTask = null;

        /// <summary>
        /// Construct socket and open connection to a specified server.
        /// </summary>
        /// <param name="log">Logging facility for verbose messaging of actions.</param>
        /// <param name="endpoint">The IP endpoint to connect to.</param>
        /// <param name="maximumReconnectionTimeout">The maximum time to wait when backing off on reconnection attempts.</param>
        public KafkaTcpSocket(IKafkaLog log, KafkaEndpoint endpoint, TimeSpan? maximumReconnectionTimeout = null)
        {
            _log = log;
            _endpoint = endpoint;
            _maximumReconnectionTimeout = maximumReconnectionTimeout ?? TimeSpan.FromMinutes(MaxReconnectionTimeoutMinutes);
        }

        #region Interface Implementation...
        /// <summary>
        /// The IP Endpoint to the server.
        /// </summary>
        public KafkaEndpoint Endpoint { get { return _endpoint; } }

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
        /// <returns>Returns Task handle to the write operation with size of written bytes..</returns>
        public Task<int> WriteAsync(byte[] buffer)
        {
            return WriteAsync(buffer, _disposeToken.Token);
        }


        /// <summary>
        /// Write the buffer data to the server.
        /// </summary>
        /// <param name="buffer">The buffer data to send.</param>
        /// <param name="cancellationToken">A cancellation token which will cancel the request.</param>
        /// <returns>Returns Task handle to the write operation with size of written bytes..</returns>
        public Task<int> WriteAsync(byte[] buffer, CancellationToken cancellationToken)
        {
            return EnsureWriteAsync(buffer, 0, buffer.Length, cancellationToken);
        }
        #endregion

        private async Task<int> EnsureWriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            try
            {
                var netStream = await GetStreamAsync().ConfigureAwait(false);
                using (await _writeLock.LockAsync(cancellationToken).ConfigureAwait(false))
                {
                    StatisticsTracker.IncrementActiveWrite();
                    //writing to network stream is not thread safe
                    //https://msdn.microsoft.com/en-us/library/z2xae4f4.aspx
                    await netStream.WriteAsync(buffer, offset, count, cancellationToken).ConfigureAwait(false);
                    StatisticsTracker.RecordNetworkWrite(_endpoint, buffer.Length);
                    StatisticsTracker.DecrementActiveWrite();
                    return buffer.Length;
                }
            }
            catch
            {
                if (_disposeToken.IsCancellationRequested) throw new ObjectDisposedException("Object is disposing.");
                throw;
            }
        }

        private async Task<byte[]> EnsureReadAsync(int readSize, CancellationToken cancellationToken)
        {
            try
            {
                var result = new List<byte>();
                var bytesReceived = 0;

                while (bytesReceived < readSize)
                {
                    readSize = readSize - bytesReceived;
                    var buffer = new byte[readSize];

                    var netStream = await GetStreamAsync().ConfigureAwait(false);

                    using (await _readLock.LockAsync(cancellationToken).ConfigureAwait(false))
                    {
                        //reading from network stream is not thread safe
                        //https://msdn.microsoft.com/en-us/library/z2xae4f4.aspx
                        bytesReceived = await netStream.ReadAsync(buffer, 0, readSize, cancellationToken)
                            .WithCancellation(cancellationToken).ConfigureAwait(false);

                        if (bytesReceived <= 0)
                        {
                            await Disconnect().ConfigureAwait(false); //_client is dead, clean it up and throw
                            throw new ServerDisconnectedException(string.Format("Lost connection to server: {0}", _endpoint));
                        }

                        result.AddRange(buffer.Take(bytesReceived));
                    }
                }
                return result.ToArray();
            }
            catch
            {
                if (_disposeToken.IsCancellationRequested) throw new ObjectDisposedException("Object is disposing.");

                //if an exception made us lose a connection throw disconnected exception
                if (_client != null && _client.Connected == false) throw new ServerDisconnectedException(string.Format("Lost connection to server: {0}", _endpoint));

                throw;
            }
        }

        private async Task<NetworkStream> GetStreamAsync()
        {
            //using a semaphore here to allow async waiting rather than blocking locks
            using (await _clientLock.LockAsync(_disposeToken.Token).ConfigureAwait(false))
            {
                if ((_client == null || _client.Connected == false) && !_disposeToken.IsCancellationRequested)
                {
                    _client = await ReEstablishConnectionAsync().ConfigureAwait(false);
                }

                return _client.GetStream();
            }
        }

        /// <summary>
        /// (Re-)establish the Kafka server connection.
        /// Assumes that the caller has already obtained the <c>_clientLock</c>
        /// </summary>
        private async Task<TcpClient> ReEstablishConnectionAsync()
        {
            var attempts = 1;
            var reconnectionDelay = DefaultReconnectionTimeout;
            _log.WarnFormat("No connection to:{0}.  Attempting to re-connect...", _endpoint);

            _client = null;

            while (_disposeToken.IsCancellationRequested == false)
            {
                try
                {
                    if (OnReconnectionAttempt != null) OnReconnectionAttempt(attempts++);
                    _client = new TcpClient();
                    await _client.ConnectAsync(_endpoint.Endpoint.Address, _endpoint.Endpoint.Port).ConfigureAwait(false);
                    _log.WarnFormat("Connection established to:{0}.", _endpoint);
                    return _client;
                }
                catch
                {
                    reconnectionDelay = reconnectionDelay * DefaultReconnectionTimeoutMultiplier;
                    reconnectionDelay = Math.Min(reconnectionDelay, (int)_maximumReconnectionTimeout.TotalMilliseconds);

                    _log.WarnFormat("Failed re-connection to:{0}.  Will retry in:{1}", _endpoint, reconnectionDelay);
                }

                await Task.Delay(TimeSpan.FromMilliseconds(reconnectionDelay), _disposeToken.Token).ConfigureAwait(false);
            }

            return _client;
        }

        private async Task Disconnect()
        {
            using (await _clientLock.LockAsync(_disposeToken.Token).ConfigureAwait(false))
            using (_client)
            {
                _client = null;
            }
        }

        public void Dispose()
        {
            if (Interlocked.Increment(ref _disposeCount) != 1) return;
            if (_disposeToken != null) _disposeToken.Cancel();

            using (_disposeToken)
            using (_client)
            using (_readLock)
            using (_writeLock)
            {
                if (_clientConnectingTask != null)
                {
                    _clientConnectingTask.Wait(TimeSpan.FromSeconds(5));
                }
            }
        }
    }
}
