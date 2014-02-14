using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Kafka.Common;

namespace Kafka.Protocol
{
    public class Protocol
    {
        private const Int16 API_VERSION = 0;

        public byte[] EncodeHeader(string clientId, int correlationId, Int16 requestKey)
        {
            return PackArray(requestKey.ToBytes(), 
                      API_VERSION.ToBytes(),
                      correlationId.ToBytes(),
                      ((Int16) clientId.Length).ToBytes(),
                      clientId.ToBytes());
        }
        
        private byte[] PackArray(params byte[][] buffers)
        {
            var size = buffers.Sum(x => x.Length);
            var buffer = new byte[size];

            int offset = 0;
            foreach (var bArray in buffers)
            {
                Buffer.BlockCopy(bArray, 0, buffer, offset, bArray.Length);
                offset += bArray.Length;
            }

            return buffer;
        }
    }
}
