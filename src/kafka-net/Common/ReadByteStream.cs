using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace KafkaNet.Common
{
    /// <summary>
    /// This class wraps a collection of bytes and provides a set of helper functions for reading
    /// Big Endian to Little Endian types off the collection of bytes[]
    /// </summary>
    /// <remarks>
    /// TODO Currently this class is hard coded to Big to Little endian.  
    /// We should technically check for the system endianness and convert accordingly.
    /// </remarks>
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
        public bool HasData { get { return _stream.Position < _stream.Length; } }

        public byte ReadByte()
        {
            return ReadBytes(1).First();
        }

        public int ReadInt()
        {
            return BitConverter.ToInt32(ReadBytes(4), 0);
        }

        public long ReadLong()
        {
            return BitConverter.ToInt64(ReadBytes(8), 0);
        }

        public Int16 ReadInt16()
        {
            return BitConverter.ToInt16(ReadBytes(2), 0);
        }

        public byte[] ReadBytes(int size)
        {
            return ReadBytesFromStream(size).Reverse().ToArray();
        }

        public string ReadString(int size)
        {
            return Encoding.Default.GetString(ReadBytesFromStream(size));
        }

        public string ReadInt16String()
        {
            var size = ReadInt16();
            if (size == -1) return null;
            return ReadString(size);
        }

        public string ReadIntString()
        {
            var size = ReadInt();
            if (size == -1) return null;
            return ReadString(size);
        }

        public byte[] ReadInt16PrefixedBytes()
        {
            var size = ReadInt16();
            return ReadBytesFromStream(size);
        }

        public byte[] ReadIntPrefixedBytes()
        {
            var size = ReadInt();
            return ReadBytesFromStream(size);
        }

        public byte[] ReadToEnd()
        {
            var size = (int)(_stream.Length - _stream.Position);
            var buffer = new byte[size];
            _stream.Read(buffer, 0, size);
            return buffer;
        }

        public byte[] ReadBytesFromStream(int size)
        {
            var bytesLeft = Convert.ToInt32(Math.Min(size, _stream.Length - _stream.Position));

            var buffer = new byte[bytesLeft];

            _stream.Read(buffer, 0, bytesLeft);

            return buffer;
        }
    }
}
