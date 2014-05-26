using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaNet
{
    public interface IKafkaTcpSocket : IDisposable
    {
        Uri ClientUri { get; }
        Task<byte[]> ReadAsync(int readSize);
        Task<byte[]> ReadAsync(int readSize, CancellationToken cancellationToken);

        Task WriteAsync(byte[] buffer, int offset, int count);
        Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken);
    }
}
