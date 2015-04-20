using SimpleKafka.Protocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleKafka
{
    internal class KafkaDecoder
    {
        private int offset;
        public int Offset { get { return offset; } }

        public void SetOffset(int offset)
        {
            this.offset = offset;
        }

        private int length;
        public int Length {  get { return length; } }

        private readonly byte[] buffer;
        public byte[] Buffer { get { return buffer; } }


        public KafkaDecoder(byte[] buffer) : this(buffer, 0, buffer.Length) { }
        public KafkaDecoder(byte[] buffer, int offset, int length)
        {
            this.buffer = buffer;
            this.length = length;
            this.offset = offset;
        }

        public void Reset(int length)
        {
            offset = 0;
            this.length = length;
        }

        public int Available {  get { return length - offset; } }

        public long ReadInt64()
        {
            unchecked
            {
                return
                    ((long)buffer[offset++] << 56) |
                    ((long)buffer[offset++] << 48) |
                    ((long)buffer[offset++] << 40) |
                    ((long)buffer[offset++] << 32) |
                    ((long)buffer[offset++] << 24) |
                    ((long)buffer[offset++] << 16) |
                    ((long)buffer[offset++] << 8) |
                    ((long)buffer[offset++]);
            }
        }

        public int ReadInt32()
        {
            unchecked
            {
                return (buffer[offset++] << 24) |
                    (buffer[offset++] << 16) |
                    (buffer[offset++] << 8) |
                    (buffer[offset++]);
            }
        }

        public uint ReadUInt32()
        {
            unchecked
            {
                return 
                    ((uint)buffer[offset++] << 24) |
                    ((uint)buffer[offset++] << 16) |
                    ((uint)buffer[offset++] << 8) |
                    ((uint)buffer[offset++]);
            }
        }

        public short ReadInt16()
        {
            unchecked
            {
                return (short)(
                    (buffer[offset++] << 8) |
                    (buffer[offset++])
                    );
            }
        }

        public ErrorResponseCode ReadErrorResponseCode()
        {
            return (ErrorResponseCode)ReadInt16();
        }

        public string ReadString()
        {
            var length = ReadInt16();
            if (length == -1)
            {
                return null;
            }
            var result = Encoding.UTF8.GetString(buffer, offset, length);
            offset += length;
            return result;
        }

        public byte ReadByte()
        {
            return buffer[offset++];
        }

        internal byte[] ReadBytes()
        {
            var length = ReadInt32();
            if (length == -1)
            {
                return null;
            }
            var result = new byte[length];
            Array.Copy(buffer, offset, result, 0, length);
            offset += length;
            return result;
        }
    }
}
