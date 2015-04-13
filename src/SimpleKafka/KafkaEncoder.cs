using SimpleKafka.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleKafka
{
    internal class KafkaEncoder
    {
        private int offset;
        public int Offset {  get { return offset; } }

        public KafkaEncoder SetOffset(int offset)
        {
            this.offset = offset;
            return this;
        }

        private readonly byte[] buffer;
        public byte[] Buffer {  get { return buffer; } }

        public KafkaEncoder(byte[] buffer, int offset = 0)
        {
            this.offset = offset;
            this.buffer = buffer;
        }

        public KafkaEncoder Reset()
        {
            offset = 0;
            return this;
        }

        public KafkaEncoder Write(long value)
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
            return this;
        }
        public KafkaEncoder Write(int value)
        {
            unchecked
            {
                buffer[offset++] = (byte)((value >> 24));
                buffer[offset++] = (byte)((value >> 16));
                buffer[offset++] = (byte)((value >> 8));
                buffer[offset++] = (byte)(value);
            }
            return this;
        }

        public KafkaEncoder Write(uint value)
        {
            unchecked
            {
                buffer[offset++] = (byte)((value >> 24));
                buffer[offset++] = (byte)((value >> 16));
                buffer[offset++] = (byte)((value >> 8));
                buffer[offset++] = (byte)(value);
            }
            return this;
        }

        public KafkaEncoder Write(short value)
        {
            unchecked
            {
                buffer[offset++] = (byte)((value >> 8));
                buffer[offset++] = (byte)(value);
            }
            return this;
        }

        public KafkaEncoder Write(byte value)
        {
            buffer[offset++] = value;
            return this;
        }

        public KafkaEncoder Write(string data)
        {
            if (data == null)
            {
                Write((short)-1);
            }
            else
            {
                var bytesWritten = Encoding.UTF8.GetBytes(data, 0, data.Length, buffer, offset + 2);
                Write((short)bytesWritten);
                offset += bytesWritten;
            }
            return this;
        }

        public KafkaEncoder Write(byte[] data)
        {
            if (data == null)
            {
                    Write(-1);
            }
            else
            {
                Write(data.Length);
                Array.Copy(data, 0, buffer, offset, data.Length);
                offset += data.Length;
            }
            return this;
        }

        public int PrepareForCrc()
        {
            offset += 4;
            return offset;
        }

        public KafkaEncoder CalculateCrc(int crcMarker)
        {
            var crc = Crc32Provider.Compute(buffer, crcMarker, offset - crcMarker);
            var current = offset;
            offset = crcMarker - 4;
            Write(crc);
            offset = current;
            return this;
        }

        public int PrepareForLength()
        {
            offset += 4;
            return offset;
        }

        public KafkaEncoder WriteLength(int lengthMarker)
        {
            var current = offset;
            var length = offset - lengthMarker;
            offset = lengthMarker - 4;
            Write(length);
            offset = current;
            return this;
        }

    }
}
