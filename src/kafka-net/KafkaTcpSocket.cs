﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.Threading;
using KafkaNet.Common;
using KafkaNet.Model;
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

        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private readonly IKafkaLog _log;
        private readonly KafkaEndpoint _endpoint;
        private readonly int? _maximumReconnectionTimeoutMs;

        private readonly SemaphoreSlim _clientSemaphoreSlim = new SemaphoreSlim(1, 1);
        private TcpClient _client;
        private int _disposeCount;
        private readonly Task _clientConnectingTask = null;

        /// <summary>
        /// Construct socket and open connection to a specified server.
        /// </summary>
        /// <param name="log">Logging facility for verbose messaging of actions.</param>
        /// <param name="endpoint">The IP endpoint to connect to.</param>
        /// <param name="maximumReconnectionTimeoutMs">Maxmimum reconnection timeout</param>
        public KafkaTcpSocket(IKafkaLog log, KafkaEndpoint endpoint, int? maximumReconnectionTimeoutMs = 0)
        {
            _log = log;
            _endpoint = endpoint;
            _maximumReconnectionTimeoutMs = maximumReconnectionTimeoutMs;
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
        /// <returns>Returns Task handle to the write operation.</returns>
        public Task WriteAsync(byte[] buffer)
        {
            return WriteAsync(buffer, _disposeToken.Token);
        }


        /// <summary>
        /// Write the buffer data to the server.
        /// </summary>
        /// <param name="buffer">The buffer data to send.</param>
        /// <param name="cancellationToken">A cancellation token which will cancel the request.</param>
        /// <returns>Returns Task handle to the write operation.</returns>
        public Task WriteAsync(byte[] buffer, CancellationToken cancellationToken)
        {
            return EnsureWriteAsync(buffer, 0, buffer.Length, cancellationToken);
        }
        #endregion

        private async Task EnsureWriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            try
            {
                var client = await GetClientAsync();
                await client.GetStream().WriteAsync(buffer, offset, count, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch
            {
                if (_disposeToken.IsCancellationRequested) throw new ObjectDisposedException("Object is disposing.");
                throw;
            }
        }

        private async Task<byte[]> EnsureReadAsync(int readSize, CancellationToken token)
        {
            var cancelTaskToken = new CancellationTokenRegistration();
            try
            {
                var result = new List<byte>();
                var bytesReceived = 0;

                while (bytesReceived < readSize)
                {
                    readSize = readSize - bytesReceived;
                    var buffer = new byte[readSize];

                    var client = await GetClientAsync();

                    bytesReceived = await client.GetStream().ReadAsync(buffer, 0, readSize, token).WithCancellation(token);

                    if (bytesReceived <= 0)
                    {
                        Disconnect(); //_client is dead, clean it up and throw
                        throw new ServerDisconnectedException(string.Format("Lost connection to server: {0}", _endpoint));
                    }

                    result.AddRange(buffer.Take(bytesReceived));
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
            finally
            {
                using (cancelTaskToken) { }
            }
        }

        private async Task<TcpClient> GetClientAsync()
        {
            //using a semaphore here to allow async waiting rather than blocking locks
            await _clientSemaphoreSlim.WaitAsync(_disposeToken.Token);
            if (_client == null || _client.Connected == false)
            {
                _client = await ReEstablishConnectionAsync();
            }
            _clientSemaphoreSlim.Release();
            return _client;
        }

        private async Task<TcpClient> ReEstablishConnectionAsync()
        {
            var attempts = 1;
            var reconnectionDelay = DefaultReconnectionTimeout;
            _log.WarnFormat("No connection to:{0}.  Attempting to re-connect...", _endpoint);

            //clean up existing client
            Disconnect();

            while (_disposeToken.IsCancellationRequested == false)
            {
                try
                {
                    if (OnReconnectionAttempt != null) OnReconnectionAttempt(attempts++);
                    _client = new TcpClient();
                    await _client.ConnectAsync(_endpoint.Endpoint.Address, _endpoint.Endpoint.Port);
                    _log.WarnFormat("Connection established to:{0}.", _endpoint);
                    return _client;
                }
                catch
                {
                    reconnectionDelay = reconnectionDelay * DefaultReconnectionTimeoutMultiplier;
                    if (_maximumReconnectionTimeoutMs.HasValue && reconnectionDelay >= _maximumReconnectionTimeoutMs)
                    {
                        reconnectionDelay = _maximumReconnectionTimeoutMs.Value;
                    }

                    _log.WarnFormat("Failed re-connection to:{0}.  Will retry in:{1}", _endpoint, reconnectionDelay);
                }

                await Task.Delay(TimeSpan.FromMilliseconds(reconnectionDelay), _disposeToken.Token);
            }

            return _client;
        }

        private void Disconnect()
        {
            using (_client) { _client = null; }
        }

        public void Dispose()
        {
            if (Interlocked.Increment(ref _disposeCount) != 1) return;
            if (_disposeToken != null) _disposeToken.Cancel();

            using (_disposeToken)
            using (_client)
            {
                if (_clientConnectingTask != null)
                {
                    _clientConnectingTask.Wait(TimeSpan.FromSeconds(5));
                }
            }
        }
    }
}
