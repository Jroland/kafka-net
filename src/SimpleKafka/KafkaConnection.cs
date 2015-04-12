using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleKafka
{
    public class KafkaConnection : IDisposable
    {
        internal static async Task<KafkaConnection> CreateAsync(IPEndPoint serverEndpoint, CancellationToken token)
        {
            var client = new TcpClient(serverEndpoint.AddressFamily);
            await client.ConnectAsync(serverEndpoint.Address, serverEndpoint.Port).ConfigureAwait(false);
            return new KafkaConnection(serverEndpoint, client);

        }
        private readonly SemaphoreSlim clientLock = new SemaphoreSlim(1);

        private readonly IPEndPoint serverEndpoint;
        public IPEndPoint ServerEndpoint {  get { return serverEndpoint; } }
        private readonly TcpClient client;
        private readonly byte[] buffer;
        private readonly NetworkStream stream;
        private KafkaDecoder decoder;
        private KafkaEncoder encoder;

        private KafkaConnection(IPEndPoint serverEndpoint, TcpClient client, int bufferSize = 65536)
        {
            this.serverEndpoint = serverEndpoint;
            this.client = client;
            this.stream = client.GetStream();
            this.buffer = new byte[bufferSize];
            decoder = new KafkaDecoder(buffer);
            encoder = new KafkaEncoder(buffer);
        }


        private async Task<int> ReceiveResponseAsync(CancellationToken token)
        {
            await stream.ReadFullyAsync(buffer, 0, 4, token).ConfigureAwait(false);
            decoder.Reset(4);
            var length = decoder.ReadInt32();
            await stream.ReadFullyAsync(buffer, 0, length, token).ConfigureAwait(false);
            decoder.Reset(length);
            return length;
        }

        public async Task<T> SendRequestAsync<T>(IKafkaRequest<T> request, CancellationToken token)
        {
            await clientLock.WaitAsync(token).ConfigureAwait(false);
            try
            {
                encoder.Reset();
                var marker = encoder.PrepareForLength();
                request.Encode(ref encoder);
                encoder.WriteLength(marker);

                await stream.WriteAsync(buffer, 0, encoder.Offset, token).ConfigureAwait(false);
                if (request.ExpectResponse)
                {
                    var length = await ReceiveResponseAsync(token).ConfigureAwait(false);
                    var result = request.Decode(ref decoder);
                    return result;
                }
                else
                {
                    return default(T);
                }
            }
            finally
            {
                clientLock.Release();
            }
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    stream.Dispose();
                    client.Close();
                    clientLock.Dispose();
                }
                disposedValue = true;
            }
        }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
        }
        #endregion

    }
}
