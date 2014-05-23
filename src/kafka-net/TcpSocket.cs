using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using KafkaNet.Common;

namespace KafkaNet
{
    public class TcpSocket : ITcpSocket
    {
        private readonly object _threadLock = new object();
        private TcpClient _client;
        private string _server;
        private int _port;

        public TcpSocket(string server, int port)
        {
            _server = server;
            _port = port;
        }

        public Task<byte[]> ReadAsync(int readSize)
        {
            return GetClient().GetStream().ReadAsync(readSize);
        }
        public Task<byte[]> ReadAsync(int readSize, System.Threading.CancellationToken cancellationToken)
        {
            return GetClient().GetStream().ReadAsync(readSize, cancellationToken);
        }

        public Task WriteAsync(byte[] buffer, int offset, int count)
        {
            return GetClient().GetStream().WriteAsync(buffer, offset, count);
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
                        _client.Connect(_server, _port);
                    }
                }
            }
            return _client;
        }




       

       
    }
}
