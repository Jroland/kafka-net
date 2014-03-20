using System;
using System.Collections.Generic;
using System.Linq;

namespace KafkaNet.Common
{
    /// <summary>
    /// This class provides methods to collect, merge and manage bytes together.  
    /// There is no built in awareness of endianess in this class.  All bytes are 
    /// store in the order given.
    /// </summary>
    public class WriteByteStream
    {
        private readonly List<byte[]> _message = new List<byte[]>();

        public int Length()
        {
            return _message.Sum(x => x.Length);
        }

        public void Prepend(params byte[][] byteArrays)
        {
            _message.InsertRange(0, byteArrays);
        }

        public void Pack(params byte[][] byteArrays)
        {
            _message.AddRange(byteArrays);
        }

        public byte[] Payload()
        {
            return PackArray(_message);
        }

        private static byte[] PackArray(List<byte[]> data)
        {
            var size = data.Sum(x => x.Length);
            var buffer = new byte[size];

            int offset = 0;
            foreach (var bArray in data)
            {
                Buffer.BlockCopy(bArray, 0, buffer, offset, bArray.Length);
                offset += bArray.Length;
            }

            return buffer;
        }
    }
}