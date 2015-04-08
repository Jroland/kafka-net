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
        private readonly BigEndianReader reader;
        private readonly BigEndianWriter writer;

        private KafkaConnection(IPEndPoint serverEndpoint, TcpClient client)
        {
            this.serverEndpoint = serverEndpoint;
            this.client = client;
            var stream = client.GetStream();
            this.reader = new BigEndianReader(stream);
            this.writer = new BigEndianWriter(stream);
        }


        private async Task<byte[]> ReceiveResponseAsync(CancellationToken token)
        {
            var length = await reader.ReadInt32Async(token).ConfigureAwait(false);
            var buffer = await reader.ReadBytesAsync(length, token).ConfigureAwait(false);
            return buffer;
        }

        private async Task<byte[]> CommunicateWithClientAsync(byte[] buffer, int offset, int length, bool expectResponse, CancellationToken token)
        {
            await clientLock.WaitAsync(token).ConfigureAwait(false);
            try
            {

                await writer.WriteAsync(buffer, offset, length, token).ConfigureAwait(false);
                if (expectResponse)
                {
                    var resultBuffer = await ReceiveResponseAsync(token).ConfigureAwait(false);
                    return resultBuffer;
                }
                else
                {
                    return null;
                }
            }
            finally
            {
                clientLock.Release();
            }

        }


        public async Task<T> SendRequestAsync<T>(IKafkaRequest<T> request, CancellationToken token)
        {
            var encoded = request.Encode();
            var resultBuffer = await CommunicateWithClientAsync(encoded, 0, encoded.Length, request.ExpectResponse, token).ConfigureAwait(false);
            if (request.ExpectResponse)
            {
                var result = request.Decode(resultBuffer).Single();
                return result;
            }
            else
            {
                return default(T);
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
                    client.Close();
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
