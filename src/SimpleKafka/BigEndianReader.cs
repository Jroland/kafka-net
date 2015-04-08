using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleKafka
{
    public class BigEndianReader
    {
        private readonly Stream stream;
        private readonly byte[] commonBuffer = new byte[8];


        public BigEndianReader(Stream stream)
        {
            this.stream = stream;
        }

        private async Task ReadFullyAsync(byte[] buffer, int offset, int numberOfBytes, CancellationToken token)
        {
            while (numberOfBytes > 0)
            {
                var bytesRead = await stream.ReadAsync(buffer, offset, numberOfBytes, token).ConfigureAwait(false);
                if (bytesRead <= 0)
                {
                    throw new EndOfStreamException();
                }
                numberOfBytes -= bytesRead;
                offset += bytesRead;
            }
        }

        public async Task<int> ReadInt32Async(CancellationToken token)
        {
            var buffer = commonBuffer;
            await ReadFullyAsync(buffer, 0, 4, token).ConfigureAwait(false);
            unchecked
            {
                return (buffer[0] << 24) |
                    (buffer[1] << 16) |
                    (buffer[2] << 8) |
                    (buffer[3]);
            }
        }

        public async Task<byte[]> ReadBytesAsync(int numberOfBytes, CancellationToken token)
        {
            var buffer = new byte[numberOfBytes];
            await ReadFullyAsync(buffer, 0, numberOfBytes, token).ConfigureAwait(false);
            return buffer;
        }

    }
}
