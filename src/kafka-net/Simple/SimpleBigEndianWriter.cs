using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaNet.Simple
{
    public class SimpleBigEndianWriter
    {
        private readonly Stream stream;
        private readonly byte[] commonBuffer = new byte[8];

        public SimpleBigEndianWriter(Stream stream)
        {
            this.stream = stream;
        }

        public async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken token)
        {
            await stream.WriteAsync(buffer, offset, count, token).ConfigureAwait(false);
        }

        public async Task WriteAsync(int value, CancellationToken token)
        {
            var buffer = commonBuffer;
            unchecked
            {
                buffer[3] = (byte)(value & 0x0ff);
                value = value >> 8;
                buffer[2] = (byte)(value & 0x0ff);
                value = value >> 8;
                buffer[1] = (byte)(value & 0x0ff);
                value = value >> 8;
                buffer[0] = (byte)(value & 0x0ff);
            }
            await stream.WriteAsync(buffer, 0, 4, token).ConfigureAwait(false);
        }
    }
}
