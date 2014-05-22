namespace KafkaNet
{
    using System.Net.Sockets;
    using System.Threading;
    using System.Threading.Tasks;
    
    public static class NetworkStreamExtensions
    {
        public static Task<byte[]> ReadAsync(this NetworkStream @this, int numberOfBytesToRead, CancellationToken? token = null)
        {
            return Task.Factory.StartNew(
                () =>
                    {
                        var buffer = new byte[numberOfBytesToRead];

                        for (int readBytes, offset = 0;
                             numberOfBytesToRead > 0;
                             offset += readBytes, numberOfBytesToRead -= readBytes)
                        {
                            if (@this.DataAvailable && @this.CanRead) readBytes = @this.Read(buffer, offset, numberOfBytesToRead);
                            else
                            {
                                readBytes = 0;
                                Thread.Sleep(1);
                            }
                        }

                        return buffer;
                    }, token ?? CancellationToken.None);
        }
    }
}