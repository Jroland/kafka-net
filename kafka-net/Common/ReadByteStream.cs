using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Kafka.Common
{
    public class ReadByteStream
    {
        private readonly byte[] _payload;
        private readonly MemoryStream _stream;

        public ReadByteStream(IEnumerable<byte> payload)
        {
            _payload = payload.ToArray();
            _stream = new MemoryStream(_payload) { Position = 0 };
        }

        public byte[] Payload { get { return _payload; } }
        public long Position { get { return _stream.Position; } set { _stream.Position = 0; } }

        public byte ReadByte()
        {
            return ReadBytes(1).First();
        }

        public byte[] ReadBytes(int size)
        {
            return ReadBytesFromStream(size).Reverse().ToArray();
        }

        public string ReadString(int size)
        {
            return Encoding.UTF8.GetString(ReadBytesFromStream(size));
        }

        public string ReadIntString()
        {
            var size = BitConverter.ToInt32(ReadBytes(4), 0);
            if (size == -1) return null;
            return ReadString(size);
        }

        public byte[] ReadToEnd()
        {
            var size = (int)(_stream.Length - _stream.Position);
            var buffer = new byte[size];
            _stream.Read(buffer, 0, size);
            return buffer;
        }
        
        private byte[] ReadBytesFromStream(int size)
        {
            var buffer = new byte[size];
            _stream.Read(buffer, 0, size);
            return buffer;
        }
    }
}
