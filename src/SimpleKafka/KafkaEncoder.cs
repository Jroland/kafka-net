using SimpleKafka.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleKafka
{
    public struct KafkaEncoder
    {
        private int offset;
        public int Offset {  get { return offset; } }

        public void SetOffset(int offset)
        {
            this.offset = offset;
        }

        private readonly byte[] buffer;
        public byte[] Buffer {  get { return buffer; } }

        public KafkaEncoder(byte[] buffer, int offset = 0)
        {
            this.offset = offset;
            this.buffer = buffer;
        }

        public void Reset()
        {
            offset = 0;
        }

        public void Write(long value)
        {
            unchecked
            {
                buffer[offset++] = (byte)((value >> 56));
                buffer[offset++] = (byte)((value >> 48));
                buffer[offset++] = (byte)((value >> 40));
                buffer[offset++] = (byte)(value >> 32);
                buffer[offset++] = (byte)((value >> 24));
                buffer[offset++] = (byte)((value >> 16));
                buffer[offset++] = (byte)((value >> 8));
                buffer[offset++] = (byte)(value);
            }
        }
        public void Write(int value)
        {
            unchecked
            {
                buffer[offset++] = (byte)((value >> 24));
                buffer[offset++] = (byte)((value >> 16));
                buffer[offset++] = (byte)((value >> 8));
                buffer[offset++] = (byte)(value);
            }
        }

        public void Write(uint value)
        {
            unchecked
            {
                buffer[offset++] = (byte)((value >> 24));
                buffer[offset++] = (byte)((value >> 16));
                buffer[offset++] = (byte)((value >> 8));
                buffer[offset++] = (byte)(value);
            }
        }

        public void Write(short value)
        {
            unchecked
            {
                buffer[offset++] = (byte)((value >> 8));
                buffer[offset++] = (byte)(value);
            }
        }

        public void Write(byte value)
        {
            buffer[offset++] = value;
        }

        public void Write(string data, StringPrefixEncoding encoding = StringPrefixEncoding.Int32)
        {
            if (data == null)
            {
                switch (encoding)
                {
                    case StringPrefixEncoding.None: break;
                    case StringPrefixEncoding.Int16: Write((short)-1); break;
                    case StringPrefixEncoding.Int32: Write(-1); break;
                    default: throw new InvalidOperationException("Unknown encoding: " + encoding);
                }

            } else
            {
                int adjust;
                switch (encoding)
                {
                    case StringPrefixEncoding.None: adjust = 0; break;
                    case StringPrefixEncoding.Int16: adjust = 2; break;
                    case StringPrefixEncoding.Int32: adjust = 4; break;
                    default: throw new InvalidOperationException("Unknown encoding: " + encoding);
                }
                var bytesWritten = Encoding.UTF8.GetBytes(data, 0, data.Length, buffer, offset + adjust);
                switch (encoding)
                {
                    case StringPrefixEncoding.None: break;
                    case StringPrefixEncoding.Int16: Write((short)bytesWritten); break;
                    case StringPrefixEncoding.Int32: Write(bytesWritten); break;
                }
                offset += bytesWritten;
            }
        }

        public void Write(byte[] data, StringPrefixEncoding encoding = StringPrefixEncoding.Int32)
        {
            if (data == null)
            {
                switch (encoding)
                {
                    case StringPrefixEncoding.Int16: Write((short)-1); break;
                    default: Write(-1); break;
                }

            }
            else
            {
                switch (encoding)
                {
                    case StringPrefixEncoding.Int16: Write((short)data.Length); break;
                    default: Write(data.Length); break;
                }
                Array.Copy(data, 0, buffer, offset, data.Length);
                offset += data.Length;
            }
        }

        internal int PrepareForCrc()
        {
            offset += 4;
            return offset;
        }

        internal void CalculateCrc(int crcMarker)
        {
            var crc = Crc32Provider.Compute(buffer, crcMarker, offset - crcMarker);
            var current = offset;
            offset = crcMarker - 4;
            Write(crc);
            offset = current;
        }

        internal int PrepareForLength()
        {
            offset += 4;
            return offset;
        }

        internal void WriteLength(int lengthMarker)
        {
            var current = offset;
            var length = offset - lengthMarker;
            offset = lengthMarker - 4;
            Write(length);
            offset = current;
        }

    }
}
