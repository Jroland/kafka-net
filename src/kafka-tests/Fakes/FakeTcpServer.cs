using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace kafka_tests.Fakes
{
    public class FakeTcpServer : IDisposable
    {
        public event Action<byte[]> OnBytesReceived;
        public event Action OnClientConnected;
        public event Action OnClientDisconnected;

        private TcpClient _client;
        private readonly SemaphoreSlim _semaphoreSlim = new SemaphoreSlim(0);
        private readonly TcpListener _listener;
        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private TaskCompletionSource<bool> _clientConnectedTrigger = new TaskCompletionSource<bool>();

        private readonly Task _clientConnectionHandlerTask = null;

        public int ConnectionEventcount = 0;
        public int DisconnectionEventCount = 0;
        public Task HasClientConnected { get { return _clientConnectedTrigger.Task; } }

        public FakeTcpServer(int port)
        {
            _listener = new TcpListener(IPAddress.Any, port);
            _listener.Start();

            OnClientConnected += () =>
            {
                Interlocked.Increment(ref ConnectionEventcount);
                _clientConnectedTrigger.TrySetResult(true);
            };

            OnClientDisconnected += () =>
            {
                Interlocked.Increment(ref DisconnectionEventCount);
                _clientConnectedTrigger = new TaskCompletionSource<bool>();
            };

            _clientConnectionHandlerTask = StartHandlingClientRequestAsync();
        }

        public async Task SendDataAsync(byte[] data)
        {
            try
            {
                await _semaphoreSlim.WaitAsync();
                Console.WriteLine("FakeTcpServer: writing {0} bytes.", data.Length);
                await _client.GetStream().WriteAsync(data, 0, data.Length).ConfigureAwait(false);
            }
            finally
            {
                _semaphoreSlim.Release();
            }
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

        private async Task StartHandlingClientRequestAsync()
        {
            while (_disposeToken.IsCancellationRequested == false)
            {
                Console.WriteLine("FakeTcpServer: Accepting clients.");
                _client = await _listener.AcceptTcpClientAsync();

                Console.WriteLine("FakeTcpServer: Connected client");
                if (OnClientConnected != null) OnClientConnected();
                _semaphoreSlim.Release();

                try
                {
                    using (_client)
                    {
                        var buffer = new byte[4096];
                        var stream = _client.GetStream();

                        while (!_disposeToken.IsCancellationRequested)
                        {
                            //connect client
                            var connectTask = stream.ReadAsync(buffer, 0, buffer.Length, _disposeToken.Token);

                            var bytesReceived = await connectTask;

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
                    _semaphoreSlim.Wait(); //remove the one client
                    if (OnClientDisconnected != null) OnClientDisconnected();
                }
            }
        }

        public void Dispose()
        {
            if (_disposeToken != null) _disposeToken.Cancel();

            using (_disposeToken)
            {
                if (_clientConnectionHandlerTask != null)
                {
                    _clientConnectionHandlerTask.Wait(TimeSpan.FromSeconds(5));
                }

                _listener.Stop();
            }
        }
    }


}
