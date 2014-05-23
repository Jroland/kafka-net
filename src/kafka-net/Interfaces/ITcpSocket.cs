using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaNet
{
    public interface ITcpSocket
    {
        Task<byte[]> ReadAsync(int readSize);
        Task<byte[]> ReadAsync(int readSize, CancellationToken cancellationToken);

        Task WriteAsync(byte[] buffer, int offset, int count);
        Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken);
    }
}
