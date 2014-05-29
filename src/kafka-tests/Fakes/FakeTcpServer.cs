using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace kafka_tests.Helpers
{
    public class FakeTcpServer : IDisposable
    {
        public delegate void BytesReceivedDelegate(byte[] data);
        public delegate void ClientEventDelegate();
        public event BytesReceivedDelegate OnBytesReceived;
        public event ClientEventDelegate OnClientConnected;
        public event ClientEventDelegate OnClientDisconnected;

        private readonly SemaphoreSlim _serverCloseSemaphore = new SemaphoreSlim(0);

        private bool _interrupted = false;
        private TcpClient _client;
        private int _clientCounter;

        public int ConnectedClients { get { return _clientCounter; } }

        public FakeTcpServer() { }
        public FakeTcpServer(int port)
        {
            Start(port);
        }

        public async Task Start(int port)
        {
            var token = new CancellationTokenSource();
            var listener = new TcpListener(IPAddress.Any, port);
            try
            {
                listener.Start();

                AcceptClientsAsync(listener, token.Token);

                await _serverCloseSemaphore.WaitAsync();
            }
            finally
            {
                token.Cancel();
                listener.Stop();
            }
        }

        public void End()
        {
            _serverCloseSemaphore.Release();
        }

        public Task SendDataAsync(byte[] data)
        {
            while (_client == null) { Thread.Sleep(100); }
            return _client.GetStream().WriteAsync(data, 0, data.Length);
        }

        public Task SendDataAsync(string data)
        {
            var msg = Encoding.ASCII.GetBytes(data);
            return SendDataAsync(msg);
        }

        public void DropConnection()
        {
            if (_client != null)
            {
                using (_client)
                {
                    _client.Close();
                }

                _client = null;
            }
        }

        private async Task AcceptClientsAsync(TcpListener listener, CancellationToken token)
        {
            try
            {
                await HandleClientAsync(listener, token);
            }
            catch (Exception ex)
            {
                Console.WriteLine("FakeTcpServer had a client exception: {0}", ex.Message);
            }
        }

        private async Task HandleClientAsync(TcpListener listener, CancellationToken token)
        {
            while (_interrupted == false)
            {
                Console.WriteLine("FakeTcpServer: Accepting clients.");
                _client = await listener.AcceptTcpClientAsync().ConfigureAwait(false);

                try
                {
                    var clientIndex = Interlocked.Increment(ref _clientCounter);
                    if (OnClientConnected != null) OnClientConnected();

                    Console.WriteLine("FakeTcpServer: Connected client: {0}", clientIndex);
                    using (_client)
                    {
                        var buffer = new byte[4096];
                        var stream = _client.GetStream();

                        while (!token.IsCancellationRequested)
                        {
                            var bytesReceived = await stream.ReadAsync(buffer, 0, buffer.Length, token);
                            if (bytesReceived > 0)
                            {
                                if (OnBytesReceived != null) OnBytesReceived(buffer.Take(bytesReceived).ToArray());
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("FakeTcpServer: Client exception...  Exception:{0}", ex.Message);
                }
                finally
                {
                    Console.WriteLine("FakeTcpServer: Client Disconnected.");
                    Interlocked.Decrement(ref _clientCounter);
                    if (OnClientDisconnected != null) OnClientDisconnected();
                }
            }
        }

        public void Dispose()
        {
            _interrupted = true;
            _serverCloseSemaphore.Release();
        }
    }


}
